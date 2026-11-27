import os
import time
import uuid
import secrets
import requests
import hmac
import hashlib
import base64
import json

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, List, Dict, Any
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

AT_USERNAME = os.getenv("AT_USERNAME", "sandbox")
AT_API_KEY = os.getenv("AT_API_KEY")
AT_SENDER_ID = os.getenv("AT_SENDER_ID", "NAXNETI")

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
    sku_code: str
    target_account: str
    amount: float
    phone_number: str
    idempotency_key: Optional[str] = None

class OrderItem(BaseModel):
    sku: str
    name: Optional[str] = None
    qty: int
    price: float

class CreateOrderRequest(BaseModel):
    customer_id: str
    vendor_id: str
    items: List[OrderItem]
    delivery_address: Dict[str, Any]   # e.g. {"court": "Court B", "plot": "14", "notes": "Blue gate"}
    fulfillment_mode: str = "RIDER_DISPATCH"  # "RIDER_DISPATCH" or "SELF_PICKUP"
    phone_number: str
    idempotency_key: Optional[str] = None

class UpdateOrderStatusRequest(BaseModel):
    status: str   # "ACCEPTED", "DISPATCHED_BODA", "DELIVERED", "CANCELLED"
    rider_id: Optional[str] = None
BRIDGE_JWT_SECRET = os.getenv("BRIDGE_JWT_SECRET") or SUPABASE_KEY or "naxneti-bridge-secret"
GAME_HMAC_SECRET = os.getenv("GAME_HMAC_SECRET") or SUPABASE_KEY or "naxneti-game-secret"

class BridgeTokenRequest(BaseModel):
    user_id: str
    app_slug: str

class GameScoreRequest(BaseModel):
    user_id: str
    score: int
    session_hash: str
    signature: str   # HMAC-SHA256 hex digest of "{user_id}:{slug}:{score}:{session_hash}"

class CreateEstatePostRequest(BaseModel):
    author_id: str
    estate_id: str
    zone_code: Optional[str] = None
    tag: str         # 'ALERT', 'MARKETPLACE', 'LOST_FOUND', 'PROMO', 'GENERAL'
    content: str
    media_urls: List[str] = []

class CreateChatMessageRequest(BaseModel):
    room_id: str
    sender_id: str
    msg_type: str = "TEXT"   # 'TEXT', 'IMAGE', 'ACTION_CARD'
    body: Optional[str] = None
    action_payload: Optional[Dict[str, Any]] = None

# -------------------------------------------------------------
# BACKGROUND WORKERS: UTILITY VENDING & VENDOR SMS ALERTS
# -------------------------------------------------------------
def fulfill_utility_order_worker(utility_order_id: str, transaction_id: str):
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
                    if utility_type == "KPLC_TOKEN":
                        delivered_token = f"{secrets.randbelow(9000)+1000}-{secrets.randbelow(9000)+1000}-{secrets.randbelow(9000)+1000}-{secrets.randbelow(9000)+1000}-{secrets.randbelow(9000)+1000}"
                    else:
                        delivered_token = f"{utility_type}_DELIVERED_TO_{target}"

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
                time.sleep(2 ** (attempt - 1))


def notify_vendor_paid_order_worker(order_id: str, transaction_id: str):
    """
    Transitions paid order to PAID_AWAITING_VENDOR and sends fallback SMS alert via Africa's Talking.
    """
    order_res = supabase.table("orders").select("*").eq("id", order_id).execute()
    if not order_res.data:
        return

    order = order_res.data[0]
    if order["status"] != "PENDING_PAYMENT":
        return

    supabase.table("orders").update({
        "status": "PAID_AWAITING_VENDOR",
        "updated_at": datetime.now(timezone.utc).isoformat()
    }).eq("id", order_id).execute()

    supabase.table("transactions").update({
        "status": "FULFILLED",
        "updated_at": datetime.now(timezone.utc).isoformat()
    }).eq("id", transaction_id).execute()

    vendor_res = supabase.table("vendors").select("owner_user_id, business_name").eq("id", order["vendor_id"]).execute()
    if not vendor_res.data:
        return

    owner_id = vendor_res.data[0]["owner_user_id"]
    biz_name = vendor_res.data[0]["business_name"]
    owner_res = supabase.table("users").select("phone").eq("id", owner_id).execute()
    if not owner_res.data or not AT_API_KEY:
        return

    vendor_phone = owner_res.data[0]["phone"]
    addr = order.get("delivery_address", {})
    landmark = f"{addr.get('court', '')} Plot {addr.get('plot', '')}".strip()
    sms_text = (
        f"NaxNeti Order ({biz_name}): KSh {order['total_amount']} PAID. "
        f"Mode: {order['fulfillment_mode']}. Deliver to: {landmark}. Open app to accept."
    )

    try:
        at_url = (
            "https://africastalking.com"
            if AT_USERNAME == "sandbox"
            else "https://africastalking.com"
        )
        requests.post(
            at_url,
            headers={"apiKey": AT_API_KEY, "Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
            data={"username": AT_USERNAME, "to": vendor_phone, "message": sms_text},
            timeout=10
        )
    except Exception:
        pass

# -------------------------------------------------------------
# API GROUP A: SPRINT 1 UTILITY VENDING & IDEMPOTENT M-PESA LEDGER
# -------------------------------------------------------------
@app.post("/v1/utilities/vend")
def initiate_utility_vend(payload: UtilityVendRequest):
    idem_key = payload.idempotency_key or f"UTIL-{payload.user_id}-{uuid.uuid4()}"

    existing_txn = supabase.table("transactions").select("*").eq("idempotency_key", idem_key).execute()
    if existing_txn.data:
        txn = existing_txn.data[0]
        return {
            "status": "duplicate_ignored",
            "transaction_id": txn["id"],
            "checkout_request_id": txn["mpesa_checkout_id"],
            "gross_amount": txn["gross_amount"]
        }

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
        util_insert = supabase.table("utility_orders").insert({
            "user_id": payload.user_id,
            "transaction_id": txn_id,
            "utility_type": sku["utility_type"],
            "target_account": payload.target_account,
            "amount": float(base_amount),
            "fulfillment_status": "QUEUED"
        }).execute()
        util_order_id = util_insert.data[0]["id"]

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
    data = await request.json()

    stk_callback = data.get("Body", {}).get("stkCallback")
    if stk_callback:
        checkout_id = stk_callback.get("CheckoutRequestID")
        result_code = int(stk_callback.get("ResultCode", -1))
        receipt_no = None
        for item in stk_callback.get("CallbackMetadata", {}).get("Item", []):
            if item.get("Name") == "MpesaReceiptNumber":
                receipt_no = str(item.get("Value"))
    else:
        checkout_id = data.get("checkout_request_id") or data.get("checkout_id")
        result_code = 0 if data.get("status") == "SUCCESS" else int(data.get("ResultCode", -1))
        receipt_no = data.get("mpesa_receipt_no") or f"RCPT_{secrets.token_hex(4).upper()}"

    if not checkout_id:
        raise HTTPException(status_code=400, detail="Missing CheckoutRequestID")

    txn_res = supabase.table("transactions").select("*").eq("mpesa_checkout_id", checkout_id).execute()
    if not txn_res.data:
        raise HTTPException(status_code=404, detail="Transaction reference not found")

    txn = txn_res.data[0]

    if txn["status"] in ("PAID", "FULFILLED"):
        return {"status": "duplicate_ignored", "message": "Callback already processed"}

    if result_code != 0:
        supabase.table("transactions").update({
            "status": "FAILED",
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", txn["id"]).execute()
        return {"status": "failed", "message": "M-Pesa payment cancelled or failed"}

    supabase.table("transactions").update({
        "status": "PAID",
        "mpesa_receipt_no": receipt_no,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }).eq("id", txn["id"]).execute()

    if txn["module_source"] == "UTILITY" and txn.get("reference_id"):
        background_tasks.add_task(fulfill_utility_order_worker, txn["reference_id"], txn["id"])
    elif txn["module_source"] == "COMMERCE" and txn.get("reference_id"):
        background_tasks.add_task(notify_vendor_paid_order_worker, txn["reference_id"], txn["id"])

    return {"status": "accepted", "message": "Payment confirmed and fulfillment queued"}


# -------------------------------------------------------------
# API GROUP D: SPRINT 2 HYPER-LOCAL COMMERCE (UMOJA / BURUBURU)
# -------------------------------------------------------------
@app.post("/v1/orders")
def create_hyperlocal_order(payload: CreateOrderRequest):
    if not payload.items:
        raise HTTPException(status_code=400, detail="Order must contain at least one item")

    idem_key = payload.idempotency_key or f"ORD-{payload.customer_id}-{uuid.uuid4()}"

    existing_txn = supabase.table("transactions").select("*").eq("idempotency_key", idem_key).execute()
    if existing_txn.data:
        txn = existing_txn.data[0]
        return {
            "status": "duplicate_ignored",
            "order_id": txn["reference_id"],
            "transaction_id": txn["id"],
            "checkout_request_id": txn["mpesa_checkout_id"],
            "total_amount": txn["gross_amount"]
        }

    vendor_res = supabase.table("vendors").select("*").eq("id", payload.vendor_id).execute()
    if not vendor_res.data:
        raise HTTPException(status_code=404, detail="Vendor not found")

    vendor = vendor_res.data[0]
    if not vendor["is_open"]:
        raise HTTPException(status_code=400, detail="This neighborhood vendor is currently closed")

    items_subtotal = sum(Decimal(str(item.price)) * Decimal(str(item.qty)) for item in payload.items)
    delivery_fee = (
        Decimal("0.00")
        if payload.fulfillment_mode == "SELF_PICKUP"
        else Decimal(str(vendor["delivery_fee_flat"]))
    )
    total_amount = round(items_subtotal + delivery_fee, 2)
    checkout_request_id = f"ws_CO_{int(datetime.now(timezone.utc).timestamp())}_{secrets.token_hex(4)}"

    try:
        serialized_items = [item.model_dump() for item in payload.items]
        order_insert = supabase.table("orders").insert({
            "customer_id": payload.customer_id,
            "vendor_id": payload.vendor_id,
            "items": serialized_items,
            "delivery_address": payload.delivery_address,
            "fulfillment_mode": payload.fulfillment_mode,
            "delivery_fee": float(delivery_fee),
            "total_amount": float(total_amount),
            "status": "PENDING_PAYMENT"
        }).execute()
        order_id = order_insert.data[0]["id"]

        txn_insert = supabase.table("transactions").insert({
            "idempotency_key": idem_key,
            "user_id": payload.customer_id,
            "module_source": "COMMERCE",
            "reference_id": order_id,
            "mpesa_checkout_id": checkout_request_id,
            "gross_amount": float(total_amount),
            "platform_fee": float(delivery_fee),
            "wholesale_cost": float(items_subtotal),
            "status": "PENDING"
        }).execute()
        txn_id = txn_insert.data[0]["id"]

        return {
            "status": "pending_payment",
            "message": "Order created and M-Pesa STK Push initiated",
            "order_id": order_id,
            "transaction_id": txn_id,
            "checkout_request_id": checkout_request_id,
            "items_subtotal": float(items_subtotal),
            "delivery_fee": float(delivery_fee),
            "total_amount": float(total_amount)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/v1/orders/{order_id}/status")
def update_order_status(order_id: str, payload: UpdateOrderStatusRequest):
    allowed_states = ("ACCEPTED", "DISPATCHED_BODA", "DELIVERED", "CANCELLED")
    if payload.status not in allowed_states:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of {allowed_states}")

    order_res = supabase.table("orders").select("*").eq("id", order_id).execute()
    if not order_res.data:
        raise HTTPException(status_code=404, detail="Order not found")

    update_fields: Dict[str, Any] = {
        "status": payload.status,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    if payload.rider_id:
        update_fields["rider_id"] = payload.rider_id

    supabase.table("orders").update(update_fields).eq("id", order_id).execute()
    return {"status": "updated", "order_id": order_id, "new_state": payload.status}
# -------------------------------------------------------------
# API GROUP E: SPRINT 3 & 4 MINI-APPS, GAMING & ESTATE PULSE
# -------------------------------------------------------------
def _mint_scoped_jwt(payload: Dict[str, Any], secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    b64_h = base64.urlsafe_b64encode(json.dumps(header, separators=(",", ":")).encode()).rstrip(b"=").decode()
    b64_p = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).rstrip(b"=").decode()
    signing_input = f"{b64_h}.{b64_p}".encode()
    sig = base64.urlsafe_b64encode(hmac.new(secret.encode(), signing_input, hashlib.sha256).digest()).rstrip(b"=").decode()
    return f"{b64_h}.{b64_p}.{sig}"


@app.post("/v1/bridge/token")
def issue_miniapp_bridge_token(payload: BridgeTokenRequest):
    """
    Sprint 3 (Module 2): Issues a 15-minute scoped JWT for sandboxed WebView Mini-Apps
    after verifying the app_slug is active in the mini_apps registry.
    """
    app_res = supabase.table("mini_apps").select("*").eq("slug", payload.app_slug).eq("is_active", True).execute()
    if not app_res.data:
        raise HTTPException(status_code=404, detail="Mini-App not found or inactive")

    user_res = supabase.table("users").select("id, phone, display_name, estate_id, landmark_address").eq("id", payload.user_id).execute()
    if not user_res.data:
        raise HTTPException(status_code=404, detail="User not found")

    mini_app = app_res.data[0]
    user = user_res.data[0]
    now_ts = int(datetime.now(timezone.utc).timestamp())

    claims = {
        "sub": user["id"],
        "phone": user["phone"],
        "display_name": user.get("display_name") or "NaxNeti Resident",
        "estate_id": user.get("estate_id"),
        "app_slug": mini_app["slug"],
        "permissions": mini_app.get("permissions", ["AUTH"]),
        "iat": now_ts,
        "exp": now_ts + 900
    }
    scoped_token = _mint_scoped_jwt(claims, BRIDGE_JWT_SECRET)
    return {
        "status": "ok",
        "app_slug": mini_app["slug"],
        "entry_url": mini_app["entry_url"],
        "permissions": claims["permissions"],
        "bridge_token": scoped_token,
        "expires_in": 900
    }


@app.post("/v1/games/{slug}/score")
def submit_game_score(slug: str, payload: GameScoreRequest):
    """
    Sprint 3 (Module 4): Validates HMAC-SHA256 score signature to block spoofing,
    records score for the current ISO tournament week, and returns estate top 10.
    """
    expected_msg = f"{payload.user_id}:{slug}:{payload.score}:{payload.session_hash}".encode()
    expected_sig = hmac.new(GAME_HMAC_SECRET.encode(), expected_msg, hashlib.sha256).hexdigest()

    if not hmac.compare_digest(expected_sig, payload.signature.lower()):
        raise HTTPException(status_code=403, detail="Invalid score signature")

    app_res = supabase.table("mini_apps").select("id").eq("slug", slug).eq("app_type", "GAME").eq("is_active", True).execute()
    if not app_res.data:
        raise HTTPException(status_code=404, detail="Game not found or inactive")

    user_res = supabase.table("users").select("estate_id").eq("id", payload.user_id).execute()
    if not user_res.data or not user_res.data[0].get("estate_id"):
        raise HTTPException(status_code=400, detail="User must select an estate before competing")

    game_id = app_res.data[0]["id"]
    estate_id = user_res.data[0]["estate_id"]
    iso_year, iso_week, _ = datetime.now(timezone.utc).isocalendar()
    tournament_week = f"{iso_year}-W{iso_week:02d}"

    supabase.table("game_scores").insert({
        "mini_app_id": game_id,
        "user_id": payload.user_id,
        "estate_id": estate_id,
        "score": payload.score,
        "tournament_week": tournament_week
    }).execute()

    top_res = (
        supabase.table("game_scores")
        .select("user_id, score, created_at")
        .eq("mini_app_id", game_id)
        .eq("estate_id", estate_id)
        .eq("tournament_week", tournament_week)
        .order("score", desc=True)
        .limit(10)
        .execute()
    )

    return {
        "status": "recorded",
        "tournament_week": tournament_week,
        "estate_leaderboard": top_res.data
    }


@app.get("/v1/feed/estate")
def get_estate_feed(estate_id: str, tag: Optional[str] = None, limit: int = 30):
    """
    Sprint 4 (Module 5): Fetches geofenced Estate Pulse community posts.
    """
    query = (
        supabase.table("estate_posts")
        .select("*")
        .eq("estate_id", estate_id)
        .order("created_at", desc=True)
        .limit(min(limit, 50))
    )
    if tag:
        query = query.eq("tag", tag.upper())

    res = query.execute()
    return {"status": "ok", "estate_id": estate_id, "posts": res.data}


@app.post("/v1/feed/estate")
def create_estate_post(payload: CreateEstatePostRequest):
    allowed_tags = ("ALERT", "MARKETPLACE", "LOST_FOUND", "PROMO", "GENERAL")
    clean_tag = payload.tag.upper()
    if clean_tag not in allowed_tags:
        raise HTTPException(status_code=400, detail=f"Tag must be one of {allowed_tags}")

    res = supabase.table("estate_posts").insert({
        "author_id": payload.author_id,
        "estate_id": payload.estate_id,
        "zone_code": payload.zone_code,
        "tag": clean_tag,
        "content": payload.content.strip(),
        "media_urls": payload.media_urls
    }).execute()

    return {"status": "created", "post": res.data[0]}


@app.post("/v1/chat/messages")
def send_chat_message(payload: CreateChatMessageRequest):
    """
    Sprint 4 (Module 5): Persists text, compressed image URLs, or interactive ACTION_CARDs
    (PAYMENT_SPLIT, ORDER_THREAD, MINI_APP_SHARE) for offline-first sync.
    """
    if payload.msg_type not in ("TEXT", "IMAGE", "ACTION_CARD"):
        raise HTTPException(status_code=400, detail="Invalid msg_type")

    res = supabase.table("chat_messages").insert({
        "room_id": payload.room_id,
        "sender_id": payload.sender_id,
        "msg_type": payload.msg_type,
        "body": payload.body,
        "action_payload": payload.action_payload
    }).execute()

    return {"status": "sent", "message": res.data[0]}

# -------------------------------------------------------------
# LEGACY WALLET TOP-UP ENDPOINTS (PRESERVED FOR EXISTING APP)
# -------------------------------------------------------------
@app.post("/api/v1/payments/stkpush")
def initiate_mpesa_push(payload: STKPushRequest):
    try:
        checkout_id = f"MPESA_CHKT_{int(datetime.now(timezone.utc).timestamp())}"
        supabase.table("wallet_transactions").insert({
            "user_id": payload.user_id,
            "mpesa_checkout_id": checkout_id,
            "amount": payload.amount,
            "transaction_type": "top-up"
        }).execute()
        return {"status": "success", "message": "STK Push initialized", "checkout_id": checkout_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/payments/callback")
async def mpesa_callback(request: Request):
    data = await request.json()
    checkout_id = data.get("checkout_id")
    status = data.get("status")

    if status == "SUCCESS":
        txn_res = supabase.table("wallet_transactions").select("*").eq("mpesa_checkout_id", checkout_id).execute()
        if not txn_res.data:
            raise HTTPException(status_code=404, detail="Transaction reference not found")

        txn = txn_res.data[0]
        user_id = txn["user_id"]
        topup_amount = Decimal(str(txn["amount"]))

        user_res = supabase.table("users").select("wallet_balance").eq("id", user_id).execute()
        if not user_res.data:
            raise HTTPException(status_code=404, detail="User not found")

        current_balance = Decimal(str(user_res.data[0]["wallet_balance"]))
        new_balance = current_balance + topup_amount
        supabase.table("users").update({"wallet_balance": float(new_balance)}).eq("id", user_id).execute()
        return {"status": "processed", "message": "Wallet credited successfully"}

    return {"status": "ignored", "message": "Transaction failed or incomplete"}

# -------------------------------------------------------------
# API GROUP B: SAUTIKIT VOICE, 4-DIGIT EXTENSION IVR & WEBRTC
# -------------------------------------------------------------
@app.post("/api/v1/voip/webrtc-token")
def mint_sautikit_webrtc_token(payload: WebRTCTokenRequest):
    if not SAUTIKIT_API_KEY:
        raise HTTPException(status_code=503, detail="SautiKit API key not configured")

    user_res = supabase.table("users").select("*").eq("id", payload.user_id).execute()
    if not user_res.data:
        raise HTTPException(status_code=404, detail="User not found")

    user = user_res.data[0]
    if not user["subscription_active"] and Decimal(str(user["wallet_balance"])) < Decimal("5.00"):
        raise HTTPException(status_code=402, detail="Insufficient KES wallet balance to start voice session")

    try:
        resp = requests.post(
            f"{SAUTIKIT_BASE}/v1/webrtc/token",
            headers={
                "Authorization": f"Bearer {SAUTIKIT_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "client_name": f"naxneti-{user['id']}",
                "role": "agent",
                "phone_number": SAUTIKIT_NUMBER
            },
            timeout=10
        )
        if not resp.ok:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"SautiKit connection failed: {str(e)}")


@app.post("/api/v1/voip/voice-callback")
async def sautikit_voice_callback(request: Request):
    body = await request.json()
    state = body.get("callSessionState", "")
    caller = body.get("callerNumber", "")
    digits = str(body.get("dtmfDigits") or body.get("digits") or "").strip()

    if state in ("Dialing", "Bridged"):
        return {"actions": []}

    if state == "Completed":
        dial_sec = int(body.get("dialDurationInSeconds") or body.get("durationInSeconds") or 0)
        dialed_user_phone = body.get("dialDestinationNumber", "")
        recording_url = body.get("recordingUrl")
        billed_kes = round(dial_sec * 0.05, 2)

        if dialed_user_phone:
            user_res = supabase.table("users").select("*").eq("phone", dialed_user_phone).execute()
            if user_res.data:
                user = user_res.data[0]
                supabase.table("call_logs").insert({
                    "user_id": user["id"],
                    "caller_number": caller,
                    "duration_seconds": dial_sec,
                    "cost_in_kes": billed_kes if not user["subscription_active"] else 0.00,
                    "is_recorded": bool(recording_url),
                    "recording_url": recording_url
                }).execute()

                if not user["subscription_active"] and billed_kes > 0:
                    new_bal = max(Decimal("0.00"), Decimal(str(user["wallet_balance"])) - Decimal(str(billed_kes)))
                    supabase.table("users").update({"wallet_balance": float(new_bal)}).eq("id", user["id"]).execute()

        return {"actions": []}

    if not digits:
        return {
            "actions": [
                {
                    "getDigits": {
                        "numDigits": 4,
                        "timeout": 10,
                        "finishOnKey": "#",
                        "nested": [
                            {
                                "say": {
                                    "text": "Karibu NaxNeti. Please enter the four digit extension of the person you are calling. Tafadhali weka namba ya upanuzi.",
                                    "language": "en-KE"
                                }
                            }
                        ]
                    }
                },
                {
                    "say": {
                        "text": "We did not receive an extension. Please try again later. Goodbye.",
                        "language": "en-KE"
                    }
                },
                {"hangup": {}}
            ]
        }

    user_res = supabase.table("users").select("*").eq("extension", digits).execute()
    if not user_res.data:
        return {
            "actions": [
                {
                    "say": {
                        "text": "That extension was not found. Please check the number and call again. Goodbye.",
                        "language": "en-KE"
                    }
                },
                {"hangup": {}}
            ]
        }

    user = user_res.data[0]

    if not user["subscription_active"] and Decimal(str(user["wallet_balance"])) < Decimal("10.00"):
        return {
            "actions": [
                {
                    "say": {
                        "text": "The subscriber you are calling is temporarily unavailable. Goodbye.",
                        "language": "en-KE"
                    }
                },
                {"hangup": {}}
            ]
        }

    return {
        "actions": [
            {
                "say": {
                    "text": "Connecting your call now. This call may be recorded for quality assurance.",
                    "language": "en-KE"
                }
            },
            {
                "dial": {
                    "number": user["phone"],
                    "callerId": SAUTIKIT_NUMBER,
                    "timeout": 30,
                    "record": "record-from-answer"
                }
            },
            {
                "say": {
                    "text": "The person you are calling did not answer. Please try again later. Goodbye.",
                    "language": "en-KE"
                }
            },
            {"hangup": {}}
        ]
    }

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
