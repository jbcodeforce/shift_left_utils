"""
Copyright 2024-2025 Confluent, Inc.
"""
from datetime import datetime
from pydantic import BaseModel, Field, field_validator,BeforeValidator
from typing import Dict, List, Optional, Any, Set

class ComputePoolInfo(BaseModel):
    id: Optional[str] = Field(default=None)
    name: str = Field(default="")
    env_id: Optional[str] = Field(default=None)
    max_cfu: int = Field(default=10)
    region: Optional[str] = Field(default=None)
    status_phase: Optional[str] = Field(default=None)
    current_cfu: Optional[int] = Field(default=None)

class ComputePoolList(BaseModel):
    created_at: Optional[datetime] = Field(default=datetime.now())
    pools: Optional[list[ComputePoolInfo]] = Field(default=[])


class Metadata(BaseModel):
    created_at: Optional[str] = Field(default=None)
    resource_name: Optional[str] = Field(default=None)
    self: Optional[str] = Field(default=None)
    updated_at: Optional[str] = Field(default=None)

class Environment(BaseModel):
    id: Optional[str] = Field(default=None)
    related: Optional[str] = Field(default=None)
    resource_name: Optional[str] = Field(default=None)

class Status(BaseModel):
    current_cfu: Optional[int] = Field(default=None)
    phase: Optional[str] = Field(default=None)

class Spec(BaseModel):
    cloud: Optional[str] = Field(default=None)
    display_name: Optional[str] = Field(default=None)
    enable_ai: Optional[bool] = Field(default=False)
    environment: Optional[Environment] = Field(default=None)
    max_cfu: Optional[int] = Field(default=None)
    region: Optional[str] = Field(default=None)

    @field_validator('enable_ai', mode='before')
    @classmethod
    def validate_enable_ai(cls, v):
        if isinstance(v, str):
            return v.lower() == 'true'
        return bool(v)

class ComputePoolResponse(BaseModel):
    api_version: Optional[str] = Field(default=None)
    id: Optional[str] = Field(default=None)
    kind: Optional[str] = Field(default=None)
    metadata: Optional[Metadata] = Field(default=None)
    spec: Optional[Spec] = Field(default=None)
    status: Optional[Status] = Field(default=None)

class ComputePoolListMetadata(BaseModel):
    first: Optional[str] = Field(default=None)
    next: Optional[str] = Field(default=None)

class ComputePoolListResponse(BaseModel):
    api_version: Optional[str] = Field(default=None)
    data: Optional[List[ComputePoolResponse]] = Field(default=[])
    kind: Optional[str] = Field(default=None)
    metadata: Optional[ComputePoolListMetadata] = Field(default=None)


