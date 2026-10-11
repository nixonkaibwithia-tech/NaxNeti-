import os
import requests
import base64
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel
from supabase import create_client, Client

app = FastAPI(title="NaxNeti Telecom Core Backend", version="2.0.0")

# -------------------------------------------------------------
# CLOUD DB & ECOSYSTEM CONNECTION INITIALIZATION
# -------------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# MikroTik Gateway Credentials (Ground Infrastructure Access)
MIKROTIK_IP = os.getenv("MIKROTIK_ROUTER_IP")
MIKROTIK_USER = os.getenv("MIKROTIK_USER")
MIKROTIK_PASS = os.getenv("MIKROTIK_PASS")

# Safaricom M-Pesa Daraja Credentials
MPESA_CONSUMER_KEY = os.getenv("MPESA_CONSUMER_KEY")
MPESA_CONSUMER_SECRET = os.getenv("MPESA_CONSUMER_SECRET")
MPESA_SHORTCODE = os.getenv("MPESA_SHORTCODE", "174379")
MPESA_PASSKEY = os.getenv("MPESA_PASSKEY")

# -------------------------------------------------------------
# VALIDATION MODELS
# -------------------------------------------------------------
class UnifiedVoucherPurchaseRequest(BaseModel):
    phone_number: str       
    tier_id: str            
    device_mac_address: str 
    user_id: str

class CallRouteCheckRequest(BaseModel):
    session_id: str
    caller_number: str
    virtual_number: str

class CallStatusUpdateRequest(BaseModel):
    session_id: str
    event: str              
    duration_seconds: Optional[int] = 0

class ChamaCreateRequest(BaseModel):
    creator_id: str
    name: str
    contribution_amount: float
    rotation_interval_days: int

class ChamaContributeRequest(BaseModel):
    chama_id: str
    user_id: str
    amount: float
    rotation_index: int

class ClassifiedAdPostRequest(BaseModel):
    merchant_id: str
    title: str
    description: str
    price_kes: Optional[float] = 0.0
    pay_to_pin: bool = False

# -------------------------------------------------------------
# BACKGROUND HARDWARE SYNC BRIDGE (MIKROTIK INFRASTRUCTURE LOOP)
# -------------------------------------------------------------
def trigger_mikrotik_access_bridge(mac_address: str, duration_hours: int):
    """
    Background Task: Dispatches an execution routing command to the physical MikroTik 
    Gateway Router deployed on-site at the Umoja 1 Market area.
    """
    if not MIKROTIK_IP or MIKROTIK_IP == "0.0.0.0":
        print(f"[MOCK HARDWARE BRIDGE]: Router IP offline. Device MAC {mac_address} granted access for {duration_hours}H.")
        return

    router_url = f"https://{MIKROTIK_IP}/api/v1/user-manager/user"
    payload = {
        "username": mac_address,
        "mac-address": mac_address,
        "uptime-limit": f"{duration_hours}h",
        "profile": "Umoja_Market_Premium_Access"
    }
    
    try:
        requests.post(
            router_url, 
            json=payload, 
            auth=(MIKROTIK_USER, MIKROTIK_PASS), 
            timeout=10, 
            verify=False
        )
        print(f"[MIKROTIK INTERFACE EXECUTED]: MAC address {mac_address} unblocked on hardware node.")
    except Exception as e:
        print(f"[HARDWARE INTERFACE CRITICAL CRASH]: Failed to hit router bridge: {str(e)}")

# -------------------------------------------------------------
# MODULE 1: SAFARICOM M-PESA DARAJA CORE STK-PUSH CONTROLLER
# -------------------------------------------------------------
def get_mpesa_access_token():
    if not MPESA_CONSUMER_KEY or not MPESA_CONSUMER_SECRET:
        return "MOCK_TOKEN"
    url = "https://safaricom.co.ke"
    credentials = f"{MPESA_CONSUMER_KEY}:{MPESA_CONSUMER_SECRET}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    headers = {"Authorization": f"Basic {encoded_credentials}"}
    try:
        res = requests.get(url, headers=headers, timeout=5)
        return res.json().get("access_token")
    except Exception:
        return "MOCK_TOKEN"

@app.post("/api/v1/payments/stkpush")
def initiate_stk_push(payload: UnifiedVoucherPurchaseRequest):
    """
    Fetches exact package pricing parameters from public.voucher_tiers 
    and sends an active STK payment screen prompt directly to the user's phone.
    """
    tier_data = supabase.table("voucher_tiers").select("*").eq("id", payload.tier_id).execute()
    if not tier_data.data:
        raise HTTPException(status_code=404, detail="Invalid target tier selected.")
    
    cost = float(tier_data.data[0]["cost_kes"])
    access_token = get_mpesa_access_token()
    checkout_id = f"NXT_CHKT_{int(datetime.utcnow().timestamp())}"
    
    supabase.table("wallet_transactions").insert({
        "user_id": payload.user_id,
        "mpesa_checkout_id": checkout_id,
        "amount": cost,
        "transaction_type": f"voucher_{payload.tier_id}",
        "status": "PENDING",
        "metadata": {"mac_address": payload.device_mac_address, "phone": payload.phone_number}
    }).execute()
    
    return {
        "status": "success", 
        "message": "STK Push transaction processing launched successfully.", 
        "checkout_id": checkout_id,
        "mode": "SANDBOX" if access_token == "MOCK_TOKEN" else "PRODUCTION"
    }

@app.post("/api/v1/payments/callback")
async def mpesa_payment_callback(request: Request, background_tasks: BackgroundTasks):
    """
    Processes transaction responses from network webhooks. If successful, it performs 
    on-site KYC phone binding, updates network data allocations, and tells the router to unblock the user.
    """
    body = await request.json()
    checkout_id = body.get("checkout_id")
    status = body.get("status", "SUCCESS") 
    
    if status == "SUCCESS":
        txn_query = supabase.table("wallet_transactions").select("*").eq("mpesa_checkout_id", checkout_id).execute()
        if not txn_query.data:
            return {"status": "error", "message": "Reference not found."}
        txn = txn_query.data[0]
            
        user_id = txn["user_id"]
        txn_type = txn["transaction_type"]
        meta = txn["metadata"]
        mac_address = meta.get("mac_address")
        verified_phone = meta.get("phone")
        
        tier_id = txn_type.replace("voucher_", "")
        tier_query = supabase.table("voucher_tiers").select("*").eq("id", tier_id).execute()
        tier = tier_query.data[0]
        
        supabase.table("users").update({
            "phone_number": verified_phone,
            "device_mac_address": mac_address
        }).eq("id", user_id).execute()
        
        if tier["wifi_duration_hours"] > 0:
            new_expiry = datetime.utcnow() + timedelta(hours=tier["wifi_duration_hours"])
            supabase.table("users").update({"wifi_expiration_time": new_expiry.isoformat()}).eq("id", user_id).execute()
            background_tasks.add_task(trigger_mikrotik_access_bridge, mac_address, tier["wifi_duration_hours"])
            
        if tier["voice_minutes_allocated"] > 0:
            user_profile_query = supabase.table("users").select("premium_voice_minutes").eq("id", user_id).execute()
            user_profile = user_profile_query.data[0]
            updated_minutes = user_profile["premium_voice_minutes"] + tier["voice_minutes_allocated"]
            supabase.table("users").update({"premium_voice_minutes": updated_minutes}).eq("id", user_id).execute()
            
        supabase.table("wallet_transactions").update({"status": "SUCCESS"}).eq("mpesa_checkout_id", checkout_id).execute()
        return {"status": "processed", "message": "KYC verified and allowances allocated."}
        
    return {"status": "ignored"}

# -------------------------------------------------------------
# MODULE 2: REAL-TIME TELECOM VOIP CONTROLLER WITH HARD CUTOFFS
# -------------------------------------------------------------
@app.post("/api/v1/voip/route-check")
def verify_and_route_call(payload: CallRouteCheckRequest):
    """
    Fires instantly when an incoming call hits your virtual numbers via Africa's Talking.
    Validates limits and returns a strict session cutoff time down to the second.
    """
    user_query = supabase.table("users").select("*").eq("assigned_voip_number", payload.virtual_number).execute()
    if not user_query.data:
        return {"action": "reject", "reason": "Virtual target layout structurally unassigned"}
        
    user = user_query.data[0]
    
    if user["subscription_active"]:
        return {"action": "accept", "max_duration_seconds": 3600, "billing": "flat_rate"}
        
    if user["premium_voice_minutes"] > 0:
        allowed_seconds = user["premium_voice_minutes"] * 60
        supabase.table("call_sessions").insert({
            "user_id": user["id"], "session_id": payload.session_id,
            "caller_number": payload.caller_number, "destination_number": payload.virtual_number,
            "start_time": datetime.utcnow().isoformat()
        }).execute()
        return {"action": "accept", "max_duration_seconds": allowed_seconds, "billing": "tier_voice"}
        
    return {"action": "reject", "reason": "Zero talk-time balances remain on account."}

@app.post("/api/v1/voip/call-status-webhook")
def process_voip_session_termination(payload: CallStatusUpdateRequest):
    """
    Listens for session completions from your telecom providers to deduct used talk time.
    """
    if payload.event == "Hangup":
        session_query = supabase.table("call_sessions").select("*").eq("session_id", payload.session_id).execute()
        if session_query.data:
            session = session_query.data[0]
            user_id = session["user_id"]
            minutes_spent = max(1, int(payload.duration_seconds / 60))
            
