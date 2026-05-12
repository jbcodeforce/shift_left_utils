"""
Copyright 2024-2026 Confluent, Inc.
"""
from datetime import datetime
from pydantic import BaseModel, Field, field_validator,BeforeValidator
from typing import Dict, List, Optional, Any, Set

class EnvironmentMetadata(BaseModel):
    created_at: Optional[str] = Field(default=None)
    resource_name: Optional[str] = Field(default=None)
    self: Optional[str] = Field(default=None)
    updated_at: Optional[str] = Field(default=None)

class StreamGovernanceConfig(BaseModel):
    package: Optional[str] = Field(default=None)

class Environment(BaseModel):
    api_version: Optional[str] = Field(default=None)
    id: Optional[str] = Field(default=None)
    kind: Optional[str] = Field(default=None)
    display_name: Optional[str] = Field(default=None)
    metadata: Optional[EnvironmentMetadata] = Field(default=None)
    stream_governance_config: Optional[StreamGovernanceConfig] = Field(default=None)



class EnvironmentListMetadata(BaseModel):
    first: Optional[str] = Field(default=None)
    last: Optional[str] = Field(default=None)
    prev: Optional[str] = Field(default=None)
    next: Optional[str] = Field(default=None)
    total_size: Optional[int] = Field(default=None)

class EnvironmentListResponse(BaseModel):
    api_version: Optional[str] = Field(default=None)
    data: Optional[List[Environment]] = Field(default=[])
    kind: Optional[str] = Field(default=None)
    metadata: Optional[EnvironmentListMetadata] = Field(default=None)


