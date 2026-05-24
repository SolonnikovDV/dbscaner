"""Persistent storage for database connection instances."""
import json
import os
import uuid
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional

from src.config import DB_CONFIG

DEFAULT_STORE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "data", "instances.json"
)


@dataclass
class Instance:
    id: str
    name: str
    host: str
    port: str
    database: str
    user: str
    password: str = field(repr=False)

    def to_config(self) -> Dict[str, str]:
        return {
            "host": self.host,
            "port": str(self.port),
            "database": self.database,
            "user": self.user,
            "password": self.password,
        }

    def to_public(self) -> Dict:
        """Safe dict for API responses (no password)."""
        return {
            "id": self.id,
            "name": self.name,
            "host": self.host,
            "port": str(self.port),
            "database": self.database,
            "user": self.user,
            "label": f"{self.host}:{self.port}/{self.database}",
        }


class InstanceStore:
    def __init__(self, path: str = DEFAULT_STORE_PATH):
        self.path = path
        self._active_id: str = ""
        self._instances: Dict[str, Instance] = {}
        self._load()

    def _load(self) -> None:
        if os.path.isfile(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._active_id = data.get("active_id", "")
            for raw in data.get("instances", []):
                inst = Instance(**raw)
                self._instances[inst.id] = inst
        else:
            self._seed_from_config()

        if not self._instances:
            self._seed_from_config()
        if self._active_id not in self._instances:
            self._active_id = next(iter(self._instances))

    def _seed_from_config(self) -> None:
        inst = Instance(
            id=str(uuid.uuid4()),
            name="Default",
            host=DB_CONFIG["host"],
            port=str(DB_CONFIG["port"]),
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG.get("password", ""),
        )
        self._instances[inst.id] = inst
        self._active_id = inst.id
        self._save()

    def _save(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        payload = {
            "active_id": self._active_id,
            "instances": [asdict(i) for i in self._instances.values()],
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

    def list_instances(self) -> List[Instance]:
        return list(self._instances.values())

    def get(self, instance_id: str) -> Optional[Instance]:
        return self._instances.get(instance_id)

    def get_active_id(self) -> str:
        return self._active_id

    def get_active(self) -> Instance:
        return self._instances[self._active_id]

    def get_active_config(self) -> Dict[str, str]:
        return self.get_active().to_config()

    def set_active(self, instance_id: str) -> Instance:
        if instance_id not in self._instances:
            raise KeyError(f"Instance {instance_id} not found")
        self._active_id = instance_id
        self._save()
        return self._instances[instance_id]

    def add(self, name: str, host: str, port: str, database: str,
            user: str, password: str) -> Instance:
        inst = Instance(
            id=str(uuid.uuid4()),
            name=name.strip() or f"{host}/{database}",
            host=host.strip(),
            port=str(port).strip(),
            database=database.strip(),
            user=user.strip(),
            password=password,
        )
        self._instances[inst.id] = inst
        self._save()
        return inst

    def update(self, instance_id: str, **fields) -> Instance:
        inst = self._instances.get(instance_id)
        if not inst:
            raise KeyError(f"Instance {instance_id} not found")
        for key in ("name", "host", "port", "database", "user", "password"):
            if key in fields and fields[key] is not None:
                val = fields[key]
                if key == "password" and val == "":
                    continue
                if key == "port":
                    val = str(val)
                setattr(inst, key, val.strip() if isinstance(val, str) else val)
        self._save()
        return inst

    def delete(self, instance_id: str) -> None:
        if len(self._instances) <= 1:
            raise ValueError("Cannot delete the last instance")
        if instance_id not in self._instances:
            raise KeyError(f"Instance {instance_id} not found")
        del self._instances[instance_id]
        if self._active_id == instance_id:
            self._active_id = next(iter(self._instances))
        self._save()
