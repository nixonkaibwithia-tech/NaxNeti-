import os
import requests
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional
from fastapi import FastAPI, HTTPException, Depends, Request
from pydantic import BaseModel
from supabase import create_client, Client

app = FastAPI(title="NaxNeti Telecom Core Backend")

# Initialize Cloud Database Connections via Env Variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") # Use Service Role for backend bypass
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------------------------------------------------------------
# PYDANTIC DATA VALIDATION MODELS
# -------------------------------------------------------------
class STKPushRequest(BaseModel):
    phone_number: str # e.g. +254712345678
    amount: float
    user_id: str

class CallRouteRequest(BaseModel):
    caller_number: str
    virtual_number: str

class WifiAuthRequest(BaseModel):
    user_id: str
    device_mac_address: str
    duration_minutes: int

# -------------------------------------------------------------
# API GROUP A: FINANCES & M-PESA PAYMENTS
# -------------------------------------------------------------
@app.post("/api/v1/payments/stkpush")
def initiate_mpesa_push(payload: STKPushRequest):
    """
    Triggers an STK Push to the user's phone via Pesapal / Flutterwave API.
    """
    # Replace with actual payment gateway integration endpoints
    gateway_url = os.getenv("PAYMENT_GATEWAY_URL")
    gateway_payload = {
        "amount": payload.amount,
        "phone": payload.phone_number,
        "reference": payload.user_id,
        "description": "NaxNeti Wallet Top-up"
    }
    
    try:
        # Mocking gateway request - in production, change to actual requests.post
        checkout_id = f"MPESA_CHKT_{int(datetime.utcnow().timestamp())}"
        
        # Log transaction as pending in Supabase
        supabase.table("wallet_transactions").insert({
            "user_id": payload.user_id,
            "mpesa_checkout_id": checkout_id,
            "amount": payload.amount,
            "transaction_type": "top-up"
        }).execute()
        
        return {"status": "success", "message": "STK Push sent successfully", "checkout_id": checkout_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/payments/callback")
async def mpesa_callback(request: Request):
    """
    Public webhook listener that handles transaction status updates from payment networks.
    """
    data = await request.json()
    checkout_id = data.get("checkout_id")
    status = data.get("status") # 'SUCCESS' or 'FAILED'
    
    if status == "SUCCESS":
        # 1. Fetch transaction record to see who paid and how much
        txn = supabase.table("wallet_transactions").select("*").eq("mpesa_checkout_id", checkout_id).execute()
        if not txn.data:
            raise HTTPException(status_code=404, detail="Transaction reference not found")
        
        user_id = txn.data[0]["user_id"]
        topup_amount = Decimal(str(txn.data[0]["amount"]))
        
        # 2. Fetch current balance
        user_data = supabase.table("users").select("wallet_balance").eq("id", user_id).execute()
        current_balance = Decimal(str(user_data.data[0]["wallet_balance"]))
        
        # 3. Calculate new balance and update user wallet
        new_balance = current_balance + topup_amount
        supabase.table("users").update({"wallet_balance": float(new_balance)}).eq("id", user_id).execute()
        
        return {"status": "processed", "message": "Wallet credited successfully"}
    
    return {"status": "ignored", "message": "Transaction failed or incomplete"}

# -------------------------------------------------------------
# API GROUP B: VOIP CALL ROUTING WEBHOOK
# -------------------------------------------------------------
@app.post("/api/v1/voip/route-check")
def verify_and_route_call(payload: CallRouteRequest):
    """
    Fired by Cloud One Kenya or Africa's Talking when an incoming call hits a virtual number.
    Checks user balances, verifies subscription state, and returns routing access.
    """
    # Look up user mapped to this specific virtual number
    user_query = supabase.table("users").select("*").eq("assigned_voip_number", payload.virtual_number).execute()
    if not user_query.data:
        return {"action": "reject", "reason": "Number unassigned"}
    
    user = user_query.data[0]
    
    # Allow call instantly if they have an active monthly package running
    if user["subscription_active"]:
        return {"action": "accept", "route_target": user["id"], "billing_type": "subscription"}
    
    # Alternatively, ensure their Pay-As-You-Go wallet balance is above minimum threshold (e.g., KES 10)
    if Decimal(str(user["wallet_balance"])) >= Decimal("10.00"):
        return {"action": "accept", "route_target": user["id"], "billing_type": "pay_as_you_go"}
        
    return {"action": "reject", "reason": "Insufficient credits"}

# -------------------------------------------------------------
# API GROUP C: WIRELESS HARDWARE CONTROLLER OPERATIONS
# -------------------------------------------------------------
@app.post("/api/v1/wifi/authorize")
def activate_mesh_session(payload: WifiAuthRequest):
    """
    Sends authorization command to your remote Ubiquiti UniFi or TP-Link Omada SDN controller.
    """
    # 1. Fetch user data to confirm eligibility
    user_data = supabase.table("users").select("*").eq("id", payload.user_id).execute()
    if not user_data.data:
        raise HTTPException(status_code=404, detail="User profile mismatch")
    
    user = user_data.data[0]
    
    # 2. Verify payment state before unlocking the hardware firewall gateway
    if not user["subscription_active"] and Decimal(str(user["wallet_balance"])) < Decimal("20.00"):
        raise HTTPException(status_code=400, detail="Payment check failed")
    
    # 3. Fire hardware API payload to Cloud SDN Router Controller
    controller_url = os.getenv("ROUTER_CONTROLLER_URL")
    controller_headers = {"Authorization": f"Bearer {os.getenv('ROUTER_API_TOKEN')}"}
    controller_payload = {
        "mac": payload.device_mac_address,
        "action": "authorize",
        "duration": payload.duration_minutes
    }
    
    try:
        # Mocking router endpoint verification - in production, toggle live requests.post
        # router_response = requests.post(controller_url, json=controller_payload, headers=controller_headers)
        
        # Log the active internet runtime session markers
        start_time = datetime.utcnow()
        end_time = start_time + timedelta(minutes=payload.duration_minutes)
        
        supabase.table("wifi_sessions").insert({
            "user_id": payload.user_id,
            "mac_address": payload.device_mac_address,
            "session_start": start_time.isoformat(),
            "session_end": end_time.isoformat()
        }).execute()
        
        # If Pay-As-You-Go, deduct the access fee (e.g., KES 20)
        if not user["subscription_active"]:
            new_bal = Decimal(str(user["wallet_balance"])) - Decimal("20.00")
            supabase.table("users").update({"wallet_balance": float(new_bal)}).eq("id", payload.user_id).execute()
            
        return {"status": "connected", "expires_at": end_time.isoformat()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Hardware authentication timeout: {str(e)}")
