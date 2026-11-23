uimport os
import requests
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from supabase import create_client, Client

app = FastAPI(title="NaxNeti Telecom Core Backend")

# -------------------------------------------------------------
# ENVIRONMENT & CLOUD CONNECTIONS
# -------------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SAUTIKIT_API_KEY = os.getenv("SAUTIKIT_API_KEY")
SAUTIKIT_NUMBER = os.getenv("SAUTIKIT_NUMBER")
SAUTIKIT_BASE = "https://sautikit.com"

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

# -------------------------------------------------------------
# API GROUP A: M-PESA & LEDGER MANAGEMENT
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
    """
    Mints a short-lived WebRTC session token from SautiKit for an authenticated NaxNeti user.
    """
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
    """
    Shared-number voice webhook:
    1. Prompts callers for a 4-digit extension.
    2. Looks up the pilot user by extension, checks balance/subscription, and bridges the call.
    3. Logs call duration, recording URL, and deducts KES on completion.
    """
    body = await request.json()
    state = body.get("callSessionState", "")
    caller = body.get("callerNumber", "")
    digits = str(body.get("dtmfDigits") or body.get("digits") or "").strip()

    # Non-interactive progress updates require no new actions
    if state in ("Dialing", "Bridged"):
        return {"actions": []}

    # 1. Terminal state: log call & bill the specific user who answered the bridged leg
    if state == "Completed":
        dial_sec = int(body.get("dialDurationInSeconds") or body.get("durationInSeconds") or 0)
        dialed_user_phone = body.get("dialDestinationNumber", "")
        recording_url = body.get("recordingUrl")
        billed_kes = round(dial_sec * 0.05, 2)  # KES 3.00/min billed per second

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

    # 2. First prompt: no digits entered yet -> ask for the 4-digit user extension
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

    # 3. Caller entered digits -> look up the matching pilot user
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

    # 4. Verify active subscription or minimum KES 10.00 balance before connecting
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

    # 5. Connect the caller to that pilot user's phone with recording notice
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

# -------------------------------------------------------------
# API GROUP C: WIRELESS HARDWARE SDN GATING
# -------------------------------------------------------------
@app.post("/api/v1/wifi/authorize")
def activate_mesh_session(payload: WifiAuthRequest):
    user_res = supabase.table("users").select("*").eq("id", payload.user_id).execute()
    if not user_res.data:
        raise HTTPException(status_code=404, detail="User profile not found")

    user = user_res.data
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

