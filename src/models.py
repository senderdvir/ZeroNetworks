from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class Launches(BaseModel):
    id: str
    name: Optional[str] = None
    date_utc: Optional[datetime] = None
    static_fire_date_utc: Optional[datetime] = None
    rocket: Optional[str] = None
    success: Optional[bool] = None
    flight_number: Optional[int] = None
    payloads: Optional[List[str]] = None
    launchpad: Optional[str] = None

class Payloads(BaseModel):
    id: str
    name: Optional[str] = None
    launch: Optional[str] = None
    mass_kg: Optional[float] = None
    mass_lbs: Optional[float] = None

class Launchpad(BaseModel):
    id: str
    name: Optional[str] = None
    full_name: Optional[str] = None
    location: Optional[dict] = None
    region: Optional[str] = None
    vehicles_launched: Optional[List[str]] = None
    launch_attempts: Optional[int] = None
    successful_launches: Optional[int] = None
    wikipedia: Optional[str] = None
    details: Optional[str] = None
