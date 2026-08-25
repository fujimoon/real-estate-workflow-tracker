import os

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class IdSettings(BaseModel):
    """独自トレースIDの採番設定"""

    # 独自ID(統合トレースID)の接頭辞。例: WFT-20260825-1A2B3C4D
    prefix: str = "WFT"
    # ランダム部の16進桁数
    random_hex_digits: int = 8


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # SQLite の保存先
    TRACKER_DB_PATH: str = "storage/tracker.db"
    # タイムライン表示に使うタイムゾーン(記録自体は常に UTC)
    TRACKER_TIMEZONE: str = "Asia/Tokyo"
    # 独自トレースIDの接頭辞(IdSettings.prefix を環境変数で上書きする用)
    TRACKER_ID_PREFIX: str = ""

    id: IdSettings = IdSettings()

    def __init__(self, **values):
        # .env ファイルが存在する場合のみ読み込み(主に開発環境用)
        if os.path.exists(".env"):
            import dotenv

            dotenv.load_dotenv(".env", override=True)
        super().__init__(**values)
        if self.TRACKER_ID_PREFIX:
            self.id.prefix = self.TRACKER_ID_PREFIX


settings = Settings()
