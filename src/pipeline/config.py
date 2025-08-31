from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

class Settings(BaseSettings):
	db_url: str
	open_meteo_lat: float = Field(default=40.7608, description="Latitude for weather location")
	open_meteo_lon: float = Field(default=-111.8910, description="Longitude for weather location")
	request_timeout_s: int = Field(default=15, description="HTTP timeout (seconds)")
	env: str = Field(default="dev", description="Logical environment: dev/test/prod")

	model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="PIPELINE_",
        extra="ignore",
    )

	@field_validator("request_timeout_s")
	@classmethod
	def validate_request(cls, v: int) -> int:
		if v <= 0:
			raise ValueError("request_timeout_s must be greater than 0")

		return v

	@property
	def db_path(self) -> Path | None:
		prefix = "sqlite:///"
		if self.db_url.startswith(prefix):
			return Path(self.db_url[len(prefix):]).resolve()
		return None
	

settings = Settings()