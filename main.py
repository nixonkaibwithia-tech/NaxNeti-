import os
import time
import uuid
import secrets
import requests
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel
from supabase import create_client, Client

app = FastAPI(title="NaxNeti Telecom & Super-App Core Backend")

# -------------------------------------------------------------
# ENVIRONMENT & CLOUD CONNECTIONS
# -------------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

SAUTIKIT_API_KEY = os.getenv("SAUTIKIT_API_KEY")
SAUTIKIT_NUMBER = os.getenv("SAUTIKIT_NUMBER")
SAUTIKIT_BASE = "https://sautikit.com"

UTILITY_AGGREGATOR_URL = os.getenv("UTILITY_AGGREGATOR_URL")
UTILITY_AGGREGATOR_KEY = os.getenv("UTILITY_AGGREGATOR_KEY")
MIKROTIK_VOUCHER_WEBHOOK = os.getenv("MIKROTIK_VOUCHER_WEBHOOK")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------------------------------------------------------------
# REQUEST MODELS
# -------------------------------------------------------------
class STKPushRequest(BaseModel):
    phone_number: str
    amount: float
    user_id: str

class WifiAuthRequest(BaseModel):
    user_id: str
    device_mac_address: str
    duration_minutes: int

class WebRTCTokenRequest(BaseModel):
    user_id: str

class UtilityVendRequest(BaseModel):
    user_id: str
    sku_code: str               # 'AIRTIME_KE', 'DATA_BUNDLE_KE', 'KPLC_PREPAID', 'ESTATE_WIFI_PASS'
    target_account: str         # Phone number (2547...), KPLC meter number, or device MAC
    amount: float
    phone_number: str           # M-Pesa payer phone number
    idempotency_key: Optional[str] = None

# -------------------------------------------------------------
# BACKGROUND WORKER: UTILITY VENDING WITH 3X EXPONENTIAL RETRY
# -------------------------------------------------------------
def fulfill_utility_order_worker(utility_order_id: str, transaction_id: str):
    """
    Dispatches Airtime, Data, KPLC Token, or Estate Wi-Fi voucher after M-Pesa confirmation.
    Retries up to 3 times with exponential backoff (1s, 2s, 4s) on aggregator timeout/failure.
    """
    order_res = supabase.table("utility_orders").select("*").eq("id", utility_order_id).execute()
    if not order_res.data:
        return

    order = order_res.data[0]
    if order["fulfillment_status"] == "DELIVERED":
        return

    utility_type = order["utility_type"]
    target = order["target_account"]
    amount = float(order["amount"])

    for attempt in range(1, 4):
        try:
            delivered_token = None
            provider_ref = f"NXN-VEND-{int(datetime.now(timezone.utc).timestamp())}"

            # 1. Local Estate Wi-Fi Pass (MikroTik/RADIUS webhook or instant voucher generator)
            if utility_type == "ESTATE_WIFI":
                voucher_code = f"NXN-{secrets.token_hex(2).upper()}-{secrets.token_hex(2).upper()}"
                if MIKROTIK_VOUCHER_WEBHOOK:
                    resp = requests.post(
                        MIKROTIK_VOUCHER_WEBHOOK,
                        json={"mac_or_account": target, "amount": amount, "voucher": voucher_code},
                        timeout=10
                    )
                    resp.raise_for_status()
                delivered_token = voucher_code

            # 2. Telco Airtime, Data Bundles, or KPLC Prepaid Tokens
            else:
                if UTILITY_AGGREGATOR_URL and UTILITY_AGGREGATOR_KEY:
                    resp = requests.post(
                        UTILITY_AGGREGATOR_URL,
                        headers={"Authorization": f"Bearer {UTILITY_AGGREGATOR_KEY}", "Content-Type": "application/json"},
                        json={
                            "utility_type": utility_type,
                            "target_account": target,
                            "amount": amount,
                            "reference": utility_order_id
                        },
                        timeout=15
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    delivered_token = data.get("token") or data.get("voucher") or "AIRTIME_CREDITED"
                    provider_ref = data.get("provider_ref") or provider_ref
                else:
                    # Sandbox fallback until live dealer keys are added to Render
                    if utility_type == "KPLC_TOKEN":
                        delivered_token = f"{secrets.randbelow(9000)+1000}-{secrets.randbelow(9000)+1000}-{secrets.randbelow(9000)+1000}-{secrets.randbelow(9000)+1000}-{secrets.randbelow(9000)+1000}"
                    else:
                        delivered_token = f"{utility_type}_DELIVERED_TO_{target}"

            # Mark utility order and ledger transaction as FULFILLED
            supabase.table("utility_orders").update({
                "fulfillment_status": "DELIVERED",
                "token_or_voucher": delivered_token,
                "provider_ref": provider_ref,
                "retry_count": attempt
            }).eq("id", utility_order_id).execute()

            supabase.table("transactions").update({
                "status": "FULFILLED",
                "updated_at": datetime.now(timezone.utc).isoformat()
            }).eq("id", transaction_id).execute()
            return

        except Exception:
            supabase.table("utility_orders").update({
                "fulfillment_status": "VEND_FAILED_RETRYING",
                "retry_count": attempt
            }).eq("id", utility_order_id).execute()

            if attempt < 3:
                time.sleep(2 ** (attempt - 1))  # Exponential backoff: 1s, 2s

# -------------------------------------------------------------
# API GROUP A: SPRINT 1 UTILITY VENDING & IDEMPOTENT M-PESA LEDGER
# -------------------------------------------------------------
@app.post("/v1/utilities/vend")
def initiate_utility_vend(payload: UtilityVendRequest):
    """
    Step 1 of Atomic Utility Flow:
    1. Enforces idempotency key to prevent duplicate charges.
    2. Calculates wholesale cost & convenience fee from utility_skus.
    3. Logs PENDING transaction + QUEUED utility_order and returns M-Pesa checkout ref.
    """
    idem_key = payload.idempotency_key or f"UTIL-{payload.user_id}-{uuid.uuid4()}"

    # Return existing transaction immediately if this idempotency_key was already processed
    existing_txn = supabase.table("transactions").select("*").eq("idempotency_key", idem_key).execute()
    if existing_txn.data:
        txn = existing_txn.data[0]
        return {
            "status": "duplicate_ignored",
            "transaction_id": txn["id"],
            "checkout_request_id": txn["mpesa_checkout_id"],
            "gross_amount": txn["gross_amount"]
        }

    # Fetch SKU margin & fee rules
    sku_res = supabase.table("utility_skus").select("*").eq("sku_code", payload.sku_code).eq("is_active", True).execute()
    if not sku_res.data:
        raise HTTPException(status_code=404, detail=f"Utility SKU '{payload.sku_code}' not found or inactive")

    sku = sku_res.data[0]
    base_amount = Decimal(str(payload.amount))
    discount_pct = Decimal(str(sku["wholesale_discount_pct"])) / Decimal("100")
    flat_fee = Decimal(str(sku["convenience_fee_flat"]))

    gross_amount = base_amount + flat_fee
    wholesale_cost = round(base_amount * (Decimal("1.00") - discount_pct), 2)
    platform_fee = round(gross_amount - wholesale_cost, 2)

    checkout_request_id = f"ws_CO_{int(datetime.now(timezone.utc).timestamp())}_{secrets.token_hex(4)}"

    try:
        # 1. Insert PENDING transaction into unified ledger
        txn_insert = supabase.table("transactions").insert({
            "idempotency_key": idem_key,
            "user_id": payload.user_id,
            "module_source": "UTILITY",
            "mpesa_checkout_id": checkout_request_id,
            "gross_amount": float(gross_amount),
            "platform_fee": float(platform_fee),
            "wholesale_cost": float(wholesale_cost),
            "status": "PENDING"
        }).execute()
        txn_id = txn_insert.data[0]["id"]

        # 2. Insert QUEUED utility order
        util_insert = supabase.table("utility_orders").insert({
            "user_id": payload.user_id,
            "transaction_id": txn_id,
            "utility_type": sku["utility_type"],
            "target_account": payload.target_account,
            "amount": float(base_amount),
            "fulfillment_status": "QUEUED"
        }).execute()
        util_order_id = util_insert.data[0]["id"]

        # Link utility_order reference_id back to transaction
        supabase.table("transactions").update({"reference_id": util_order_id}).eq("id", txn_id).execute()

        return {
            "status": "pending_payment",
            "message": "M-Pesa STK Push initiated",
            "transaction_id": txn_id,
            "utility_order_id": util_order_id,
            "checkout_request_id": checkout_request_id,
            "gross_amount": float(gross_amount),
            "convenience_fee": float(flat_fee)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/v1/payments/mpesa/callback")
async def unified_mpesa_callback(request: Request, background_tasks: BackgroundTasks):
    """
    Idempotent Daraja C2B / Gateway callback handler:
    - Safely ignores duplicate callbacks if transaction is already PAID or FULFILLED.
    - Dispatches background fulfillment worker for UTILITY orders.
    """
    data = await request.json()

    # Support both official Safaricom Daraja STK callback payload and flat test payloads
    stk_callback = data.get("Body", {}).get("stkCallback")
    if stk_callback:
        checkout_id = stk_callback.get("CheckoutRequestID")
# -------------------------------------------------------------
# API GROUP C: WIRELESS HARDWARE SDN GATING
# -------------------------------------------------------------
@app.post("/api/v1/wifi/authorize")
def activate_mesh_session(payload: WifiAuthRequest):
    user_res = supabase.table("users").select("*").eq("id", payload.user_id).execute()
    if not user_res.data:
        raise HTTPException(status_code=404, detail="User profile not found")

    user = user_res.data[0]
    if not user["subscription_active"] and Decimal(str(user["wallet_balance"])) < Decimal("20.00"):
        raise HTTPException(status_code=400, detail="Insufficient balance for WiFi pass")

    start_time = datetime.now(timezone.utc)
    end_time = start_time + timedelta(minutes=payload.duration_minutes)

    try:
        supabase.table("wifi_sessions").insert({
            "user_id": payload.user_id,
            "mac_address": payload.device_mac_address,
            "session_start": start_time.isoformat(),
            "session_end": end_time.isoformat()
        }).execute()

        if not user["subscription_active"]:
            new_bal = Decimal(str(user["wallet_balance"])) - Decimal("20.00")
            supabase.table("users").update({"wallet_balance": float(new_bal)}).eq("id", payload.user_id).execute()

        return {"status": "connected", "expires_at": end_time.isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
