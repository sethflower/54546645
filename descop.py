import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, date, time
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from platformdirs import user_data_dir
import tkinter as tk
from tkinter import messagebox, ttk

BASE_URL = "http://173.242.53.38:1000"
SCANPAK_BASE_PATH = "/scanpak"
APP_NAME = "TrackingApp"


@dataclass
class ApiError(Exception):
    message: str
    status_code: int


class LocalStore:
    def __init__(self) -> None:
        self.base_dir = user_data_dir(APP_NAME, "Tracking")
        os.makedirs(self.base_dir, exist_ok=True)
        self.state_path = os.path.join(self.base_dir, "state.json")
        self.tracking_offline_path = os.path.join(
            self.base_dir, "offline_records.json"
        )
        self.scanpak_offline_path = os.path.join(
            self.base_dir, "scanpak_offline_scans.json"
        )

    def load_state(self) -> Dict[str, Any]:
        if not os.path.exists(self.state_path):
            return {}
        try:
            with open(self.state_path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except (OSError, json.JSONDecodeError):
            return {}

    def save_state(self, data: Dict[str, Any]) -> None:
        try:
            with open(self.state_path, "w", encoding="utf-8") as handle:
                json.dump(data, handle, ensure_ascii=False, indent=2)
        except OSError:
            messagebox.showwarning(
                "Помилка", "Не вдалося зберегти локальні налаштування."
            )

    def load_offline_records(self, path: str) -> List[Dict[str, Any]]:
        if not os.path.exists(path):
            return []
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, list):
                return [record for record in data if isinstance(record, dict)]
        except (OSError, json.JSONDecodeError):
            pass
        return []

    def save_offline_records(self, path: str, records: List[Dict[str, Any]]) -> None:
        try:
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(records, handle, ensure_ascii=False, indent=2)
        except OSError:
            messagebox.showwarning(
                "Помилка", "Не вдалося зберегти офлайн-чергу."
            )


class ApiClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def _request(
        self,
        method: str,
        path: str,
        token: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        response = requests.request(
            method,
            url,
            headers=headers,
            json=payload,
            timeout=12,
        )
        return response

    @staticmethod
    def _extract_message(response: requests.Response) -> str:
        try:
            body = response.json()
            if isinstance(body, dict):
                detail = body.get("detail") or body.get("message")
                if isinstance(detail, str) and detail:
                    return detail
        except ValueError:
            pass
        return f"Помилка сервера ({response.status_code})"

    def request_json(
        self,
        method: str,
        path: str,
        token: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Any:
        response = self._request(method, path, token=token, payload=payload)
        if response.status_code != 200:
            raise ApiError(self._extract_message(response), response.status_code)
        if response.text:
            try:
                return response.json()
            except ValueError:
                raise ApiError("Некоректна відповідь сервера", response.status_code)
        return None


class OfflineQueue:
    def __init__(self, store: LocalStore, path: str) -> None:
        self.store = store
        self.path = path

    def add(self, record: Dict[str, Any]) -> None:
        records = self.store.load_offline_records(self.path)
        records.append(record)
        self.store.save_offline_records(self.path, records)

    def contains(self, key: str, value: str) -> bool:
        records = self.store.load_offline_records(self.path)
        return any(str(item.get(key, "")).strip() == value for item in records)

    def list(self) -> List[Dict[str, Any]]:
        return self.store.load_offline_records(self.path)

    def clear(self) -> None:
        self.store.save_offline_records(self.path, [])


def parse_date(value: str) -> Optional[date]:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_time(value: str) -> Optional[time]:
    try:
        return datetime.strptime(value, "%H:%M").time()
    except ValueError:
        return None


def format_datetime(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.astimezone().strftime("%d.%m.%Y %H:%M:%S")
    except ValueError:
        return value


def run_async(
    root: tk.Misc,
    func: Callable[[], Any],
    on_success: Callable[[Any], None],
    on_error: Callable[[Exception], None],
) -> None:
    def worker() -> None:
        try:
            result = func()
            root.after(0, lambda: on_success(result))
        except Exception as exc:  # noqa: BLE001
            root.after(0, lambda: on_error(exc))

    threading.Thread(target=worker, daemon=True).start()


class TrackingApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("TrackingApp")
        self.geometry("1200x760")
        self.minsize(1080, 680)
        self.store = LocalStore()
        self.state_data = self.store.load_state()
        self.api = ApiClient(BASE_URL)
        self.scanpak_api = ApiClient(f"{BASE_URL}{SCANPAK_BASE_PATH}")
        self.tracking_offline = OfflineQueue(self.store, self.store.tracking_offline_path)
        self.scanpak_offline = OfflineQueue(self.store, self.store.scanpak_offline_path)
        self._init_style()

        self.container = ttk.Frame(self, padding=16)
        self.container.pack(fill=tk.BOTH, expand=True)

        self.frames: Dict[str, ttk.Frame] = {}
        for frame_class in (
            StartFrame,
            TrackingLoginFrame,
            ScanpakLoginFrame,
            TrackingMainFrame,
            ScanpakMainFrame,
        ):
            frame = frame_class(self.container, self)
            self.frames[frame_class.__name__] = frame
            frame.grid(row=0, column=0, sticky="nsew")

        self.show_frame("StartFrame")

    def _init_style(self) -> None:
        style = ttk.Style(self)
        style.theme_use("clam")
        self.configure(bg="#F2F5FA")
        style.configure("TFrame", background="#F2F5FA")
        style.configure("Card.TFrame", background="#FFFFFF", relief="flat")
        style.configure(
            "Accent.TButton",
            background="#2563EB",
            foreground="#FFFFFF",
            padding=10,
            font=("Segoe UI", 10, "bold"),
        )
        style.map(
            "Accent.TButton",
            background=[("active", "#1D4ED8"), ("disabled", "#94A3B8")],
        )
        style.configure(
            "Secondary.TButton",
            background="#E2E8F0",
            foreground="#1E293B",
            padding=8,
        )
        style.map(
            "Secondary.TButton",
            background=[("active", "#CBD5F5")],
        )
        style.configure("Title.TLabel", font=("Segoe UI", 22, "bold"))
        style.configure("Subtitle.TLabel", font=("Segoe UI", 12))
        style.configure("Header.TLabel", font=("Segoe UI", 14, "bold"))
        style.configure("Status.TLabel", font=("Segoe UI", 11))
        style.configure("Treeview", font=("Segoe UI", 10), rowheight=24)
        style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"))

    def show_frame(self, name: str) -> None:
        frame = self.frames[name]
        frame.tkraise()
        if hasattr(frame, "refresh"):
            frame.refresh()

    def update_state(self, updates: Dict[str, Any]) -> None:
        self.state_data.update(updates)
        self.store.save_state(self.state_data)

    def clear_state(self, keys: List[str]) -> None:
        for key in keys:
            self.state_data.pop(key, None)
        self.store.save_state(self.state_data)


class StartFrame(ttk.Frame):
    def __init__(self, parent: ttk.Frame, app: TrackingApp) -> None:
        super().__init__(parent)
        self.app = app
        card = ttk.Frame(self, style="Card.TFrame", padding=40)
        card.place(relx=0.5, rely=0.5, anchor="center")

        ttk.Label(card, text="TrackingApp", style="Title.TLabel").pack(pady=(0, 10))
        ttk.Label(
            card,
            text="Оберіть модуль для роботи",
            style="Subtitle.TLabel",
        ).pack(pady=(0, 30))

        ttk.Button(
            card,
            text="Вхід до TrackingApp",
            style="Accent.TButton",
            command=lambda: app.show_frame("TrackingLoginFrame"),
        ).pack(fill=tk.X, pady=6)

        ttk.Button(
            card,
            text="Вхід до СканПак",
            style="Secondary.TButton",
            command=lambda: app.show_frame("ScanpakLoginFrame"),
        ).pack(fill=tk.X, pady=6)


class TrackingLoginFrame(ttk.Frame):
    def __init__(self, parent: ttk.Frame, app: TrackingApp) -> None:
        super().__init__(parent)
        self.app = app
        self.is_busy = tk.BooleanVar(value=False)
        self.message = tk.StringVar(value="")

        card = ttk.Frame(self, style="Card.TFrame", padding=40)
        card.place(relx=0.5, rely=0.5, anchor="center")

        ttk.Label(card, text="TrackingApp", style="Title.TLabel").pack()
        ttk.Label(
            card,
            text="Авторизація та реєстрація операторів",
            style="Subtitle.TLabel",
        ).pack(pady=(0, 20))

        self.tabs = ttk.Notebook(card)
        self.tabs.pack(fill=tk.BOTH, expand=True)

        self.login_tab = ttk.Frame(self.tabs, padding=20)
        self.register_tab = ttk.Frame(self.tabs, padding=20)
        self.tabs.add(self.login_tab, text="Вхід")
        self.tabs.add(self.register_tab, text="Реєстрація")

        self.login_surname = tk.StringVar()
        self.login_password = tk.StringVar()

        self.register_surname = tk.StringVar()
        self.register_password = tk.StringVar()
        self.register_confirm = tk.StringVar()

        self._build_login()
        self._build_register()

        ttk.Label(card, textvariable=self.message, foreground="#DC2626").pack(
            pady=8
        )

        buttons = ttk.Frame(card)
        buttons.pack(fill=tk.X, pady=(10, 0))
        ttk.Button(
            buttons,
            text="Назад",
            style="Secondary.TButton",
            command=lambda: app.show_frame("StartFrame"),
        ).pack(side=tk.LEFT)
        ttk.Button(
            buttons,
            text="Адмін панель",
            style="Secondary.TButton",
            command=self.open_admin_panel,
        ).pack(side=tk.RIGHT)

    def _build_login(self) -> None:
        ttk.Label(self.login_tab, text="Прізвище").grid(row=0, column=0, sticky="w")
        ttk.Entry(self.login_tab, textvariable=self.login_surname).grid(
            row=1, column=0, sticky="ew", pady=(0, 12)
        )
        ttk.Label(self.login_tab, text="Пароль").grid(row=2, column=0, sticky="w")
        ttk.Entry(self.login_tab, textvariable=self.login_password, show="*").grid(
            row=3, column=0, sticky="ew", pady=(0, 12)
        )
        ttk.Button(
            self.login_tab,
            text="Увійти",
            style="Accent.TButton",
            command=self.handle_login,
        ).grid(row=4, column=0, sticky="ew")
        self.login_tab.columnconfigure(0, weight=1)

    def _build_register(self) -> None:
        ttk.Label(self.register_tab, text="Прізвище").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Entry(self.register_tab, textvariable=self.register_surname).grid(
            row=1, column=0, sticky="ew", pady=(0, 12)
        )
        ttk.Label(self.register_tab, text="Пароль").grid(row=2, column=0, sticky="w")
        ttk.Entry(self.register_tab, textvariable=self.register_password, show="*").grid(
            row=3, column=0, sticky="ew", pady=(0, 12)
        )
        ttk.Label(self.register_tab, text="Підтвердження пароля").grid(
            row=4, column=0, sticky="w"
        )
        ttk.Entry(self.register_tab, textvariable=self.register_confirm, show="*").grid(
            row=5, column=0, sticky="ew", pady=(0, 12)
        )
        ttk.Button(
            self.register_tab,
            text="Надіслати заявку",
            style="Accent.TButton",
            command=self.handle_register,
        ).grid(row=6, column=0, sticky="ew")
        self.register_tab.columnconfigure(0, weight=1)

    def _set_busy(self, busy: bool) -> None:
        self.is_busy.set(busy)

    def handle_login(self) -> None:
        surname = self.login_surname.get().strip()
        password = self.login_password.get().strip()
        if not surname or not password:
            self.message.set("Введіть прізвище та пароль")
            return

        self.message.set("")
        self._set_busy(True)

        def task() -> Dict[str, Any]:
            return self.app.api.request_json(
                "POST", "/login", payload={"surname": surname, "password": password}
            )

        def on_success(data: Dict[str, Any]) -> None:
            self._set_busy(False)
            token = str(data.get("token", ""))
            if not token:
                self.message.set("Сервер не повернув токен")
                return
            self.app.update_state(
                {
                    "token": token,
                    "access_level": data.get("access_level"),
                    "user_name": data.get("surname", surname),
                    "user_role": data.get("role"),
                }
            )
            self.app.show_frame("TrackingMainFrame")

        def on_error(exc: Exception) -> None:
            self._set_busy(False)
            if isinstance(exc, ApiError):
                self.message.set(exc.message)
            else:
                self.message.set("Не вдалося зʼєднатися з сервером")

        run_async(self, task, on_success, on_error)

    def handle_register(self) -> None:
        surname = self.register_surname.get().strip()
        password = self.register_password.get().strip()
        confirm = self.register_confirm.get().strip()
        if not surname or not password or not confirm:
            self.message.set("Заповніть усі поля")
            return
        if len(password) < 6:
            self.message.set("Пароль має містити щонайменше 6 символів")
            return
        if password != confirm:
            self.message.set("Паролі не співпадають")
            return

        def task() -> Any:
            return self.app.api.request_json(
                "POST", "/register", payload={"surname": surname, "password": password}
            )

        def on_success(_: Any) -> None:
            self.message.set(
                "Заявку на реєстрацію відправлено. Дочекайтесь підтвердження."
            )
            self.register_surname.set("")
            self.register_password.set("")
            self.register_confirm.set("")

        def on_error(exc: Exception) -> None:
            if isinstance(exc, ApiError):
                self.message.set(exc.message)
            else:
                self.message.set("Не вдалося відправити заявку")

        run_async(self, task, on_success, on_error)

    def open_admin_panel(self) -> None:
        password = simple_prompt(self, "Пароль адміністратора")
        if not password:
            return

        def task() -> Dict[str, Any]:
            return self.app.api.request_json(
                "POST", "/admin_login", payload={"password": password}
            )

        def on_success(data: Dict[str, Any]) -> None:
            token = str(data.get("token", ""))
            if not token:
                messagebox.showerror("Помилка", "Сервер не повернув токен")
                return
            AdminPanel(self, self.app, token)

        def on_error(exc: Exception) -> None:
            if isinstance(exc, ApiError):
                messagebox.showerror("Помилка", exc.message)
            else:
                messagebox.showerror("Помилка", "Не вдалося зʼєднатися з сервером")

        run_async(self, task, on_success, on_error)


class ScanpakLoginFrame(ttk.Frame):
    def __init__(self, parent: ttk.Frame, app: TrackingApp) -> None:
        super().__init__(parent)
        self.app = app
        self.message = tk.StringVar(value="")

        card = ttk.Frame(self, style="Card.TFrame", padding=40)
        card.place(relx=0.5, rely=0.5, anchor="center")

        ttk.Label(card, text="СканПак", style="Title.TLabel").pack()
        ttk.Label(card, text="Авторизація для Scanpak", style="Subtitle.TLabel").pack(
            pady=(0, 20)
        )

        self.tabs = ttk.Notebook(card)
        self.tabs.pack(fill=tk.BOTH, expand=True)

        self.login_tab = ttk.Frame(self.tabs, padding=20)
        self.register_tab = ttk.Frame(self.tabs, padding=20)
        self.tabs.add(self.login_tab, text="Вхід")
        self.tabs.add(self.register_tab, text="Реєстрація")

        self.login_surname = tk.StringVar()
        self.login_password = tk.StringVar()
        self.register_surname = tk.StringVar()
        self.register_password = tk.StringVar()
        self.register_confirm = tk.StringVar()

        self._build_login()
        self._build_register()

        ttk.Label(card, textvariable=self.message, foreground="#DC2626").pack(
            pady=8
        )

        buttons = ttk.Frame(card)
        buttons.pack(fill=tk.X, pady=(10, 0))
        ttk.Button(
            buttons,
            text="Назад",
            style="Secondary.TButton",
            command=lambda: app.show_frame("StartFrame"),
        ).pack(side=tk.LEFT)
        ttk.Button(
            buttons,
            text="Адмін панель",
            style="Secondary.TButton",
            command=self.open_admin_panel,
        ).pack(side=tk.RIGHT)

    def _build_login(self) -> None:
        ttk.Label(self.login_tab, text="Прізвище").grid(row=0, column=0, sticky="w")
        ttk.Entry(self.login_tab, textvariable=self.login_surname).grid(
            row=1, column=0, sticky="ew", pady=(0, 12)
        )
        ttk.Label(self.login_tab, text="Пароль").grid(row=2, column=0, sticky="w")
        ttk.Entry(self.login_tab, textvariable=self.login_password, show="*").grid(
            row=3, column=0, sticky="ew", pady=(0, 12)
        )
        ttk.Button(
            self.login_tab,
            text="Увійти",
            style="Accent.TButton",
            command=self.handle_login,
        ).grid(row=4, column=0, sticky="ew")
        self.login_tab.columnconfigure(0, weight=1)

    def _build_register(self) -> None:
        ttk.Label(self.register_tab, text="Прізвище").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Entry(self.register_tab, textvariable=self.register_surname).grid(
            row=1, column=0, sticky="ew", pady=(0, 12)
        )
        ttk.Label(self.register_tab, text="Пароль").grid(row=2, column=0, sticky="w")
        ttk.Entry(self.register_tab, textvariable=self.register_password, show="*").grid(
            row=3, column=0, sticky="ew", pady=(0, 12)
        )
        ttk.Label(self.register_tab, text="Підтвердження пароля").grid(
            row=4, column=0, sticky="w"
        )
        ttk.Entry(self.register_tab, textvariable=self.register_confirm, show="*").grid(
            row=5, column=0, sticky="ew", pady=(0, 12)
        )
        ttk.Button(
            self.register_tab,
            text="Надіслати заявку",
            style="Accent.TButton",
            command=self.handle_register,
        ).grid(row=6, column=0, sticky="ew")
        self.register_tab.columnconfigure(0, weight=1)

    def handle_login(self) -> None:
        surname = self.login_surname.get().strip()
        password = self.login_password.get().strip()
        if not surname or not password:
            self.message.set("Введіть прізвище та пароль")
            return
        self.message.set("")

        def task() -> Dict[str, Any]:
            return self.app.scanpak_api.request_json(
                "POST", "/login", payload={"surname": surname, "password": password}
            )

        def on_success(data: Dict[str, Any]) -> None:
            token = str(data.get("token", ""))
            if not token:
                self.message.set("Сервер не повернув токен")
                return
            self.app.update_state(
                {
                    "scanpak_token": token,
                    "scanpak_user_name": data.get("surname", surname),
                    "scanpak_user_role": data.get("role"),
                }
            )
            self.app.show_frame("ScanpakMainFrame")

        def on_error(exc: Exception) -> None:
            if isinstance(exc, ApiError):
                self.message.set(exc.message)
            else:
                self.message.set("Не вдалося зʼєднатися з сервером")

        run_async(self, task, on_success, on_error)

    def handle_register(self) -> None:
        surname = self.register_surname.get().strip()
        password = self.register_password.get().strip()
        confirm = self.register_confirm.get().strip()
        if not surname or not password or not confirm:
            self.message.set("Заповніть усі поля")
            return
        if len(password) < 6:
            self.message.set("Пароль має містити щонайменше 6 символів")
            return
        if password != confirm:
            self.message.set("Паролі не співпадають")
            return

        def task() -> Any:
            return self.app.scanpak_api.request_json(
                "POST", "/register", payload={"surname": surname, "password": password}
            )

        def on_success(_: Any) -> None:
            self.message.set("Заявку на реєстрацію відправлено.")
            self.register_surname.set("")
            self.register_password.set("")
            self.register_confirm.set("")

        def on_error(exc: Exception) -> None:
            if isinstance(exc, ApiError):
                self.message.set(exc.message)
            else:
                self.message.set("Не вдалося відправити заявку")

        run_async(self, task, on_success, on_error)

    def open_admin_panel(self) -> None:
        password = simple_prompt(self, "Пароль адміністратора СканПак")
        if not password:
            return

        def task() -> Dict[str, Any]:
            return self.app.scanpak_api.request_json(
                "POST", "/admin_login", payload={"password": password}
            )

        def on_success(data: Dict[str, Any]) -> None:
            token = str(data.get("token", ""))
            if not token:
                messagebox.showerror("Помилка", "Сервер не повернув токен")
                return
            ScanpakAdminPanel(self, self.app, token)

        def on_error(exc: Exception) -> None:
            if isinstance(exc, ApiError):
                messagebox.showerror("Помилка", exc.message)
            else:
                messagebox.showerror("Помилка", "Не вдалося зʼєднатися з сервером")

        run_async(self, task, on_success, on_error)


class TrackingMainFrame(ttk.Frame):
    def __init__(self, parent: ttk.Frame, app: TrackingApp) -> None:
        super().__init__(parent)
        self.app = app
        self.status = tk.StringVar(value="")
        self.user_label = tk.StringVar(value="")
        self.role_label = tk.StringVar(value="")
        self.access_level = None

        header = ttk.Frame(self)
        header.pack(fill=tk.X)
        ttk.Label(header, text="TrackingApp", style="Header.TLabel").pack(
            side=tk.LEFT
        )
        ttk.Label(header, textvariable=self.user_label).pack(side=tk.LEFT, padx=20)
        ttk.Label(header, textvariable=self.role_label).pack(side=tk.LEFT)
        ttk.Button(
            header,
            text="Вийти",
            style="Secondary.TButton",
            command=self.logout,
        ).pack(side=tk.RIGHT)

        self.tabs = ttk.Notebook(self)
        self.tabs.pack(fill=tk.BOTH, expand=True, pady=12)

        self.scan_tab = TrackingScanTab(self.tabs, app, self.status)
        self.history_tab = HistoryTab(self.tabs, app)
        self.errors_tab = ErrorsTab(self.tabs, app)
        self.stats_tab = StatisticsTab(self.tabs, app)

        self.tabs.add(self.scan_tab, text="Сканер")
        self.tabs.add(self.history_tab, text="Історія")
        self.tabs.add(self.errors_tab, text="Помилки")
        self.tabs.add(self.stats_tab, text="Статистика")

        ttk.Label(self, textvariable=self.status, style="Status.TLabel").pack(
            pady=(0, 8)
        )

    def refresh(self) -> None:
        user = self.app.state_data.get("user_name", "оператор")
        role = self.app.state_data.get("user_role")
        access_level = self.app.state_data.get("access_level")
        self.access_level = access_level
        role_label = "👁 Перегляд"
        if role == "admin" or access_level == 1:
            role_label = "🔑 Адмін"
        elif role == "operator" or access_level == 0:
            role_label = "🧰 Оператор"
        self.user_label.set(f"Оператор: {user}")
        self.role_label.set(role_label)
        self.scan_tab.refresh()
        self.history_tab.refresh()
        self.errors_tab.refresh()
        self.stats_tab.refresh()

    def logout(self) -> None:
        self.app.clear_state(["token", "access_level", "user_name", "user_role"])
        self.app.show_frame("StartFrame")


class TrackingScanTab(ttk.Frame):
    def __init__(self, parent: ttk.Notebook, app: TrackingApp, status: tk.StringVar) -> None:
        super().__init__(parent, padding=20)
        self.app = app
        self.status = status
        self.box_var = tk.StringVar()
        self.ttn_var = tk.StringVar()
        self.inflight = tk.IntVar(value=0)

        form = ttk.Frame(self)
        form.pack(fill=tk.X)
        ttk.Label(form, text="BoxID").grid(row=0, column=0, sticky="w")
        box_entry = ttk.Entry(form, textvariable=self.box_var)
        box_entry.grid(row=1, column=0, sticky="ew", pady=(0, 12))
        ttk.Label(form, text="ТТН").grid(row=2, column=0, sticky="w")
        ttn_entry = ttk.Entry(form, textvariable=self.ttn_var)
        ttn_entry.grid(row=3, column=0, sticky="ew")
        form.columnconfigure(0, weight=1)

        action = ttk.Frame(self)
        action.pack(fill=tk.X, pady=12)
        ttk.Button(
            action,
            text="Надіслати",
            style="Accent.TButton",
            command=self.send_record,
        ).pack(side=tk.LEFT)
        ttk.Button(
            action,
            text="Синхронізувати офлайн",
            style="Secondary.TButton",
            command=self.sync_offline,
        ).pack(side=tk.LEFT, padx=8)
        ttk.Label(action, textvariable=self.inflight).pack(side=tk.RIGHT)

        box_entry.bind("<Return>", lambda _: ttn_entry.focus())
        ttn_entry.bind("<Return>", lambda _: self.send_record())

    def refresh(self) -> None:
        self.inflight.set(len(self.app.tracking_offline.list()))

    def send_record(self) -> None:
        token = self.app.state_data.get("token")
        user_name = self.app.state_data.get("user_name", "operator")
        boxid = "".join(filter(str.isdigit, self.box_var.get()))
        ttn = "".join(filter(str.isdigit, self.ttn_var.get()))
        if not boxid or not ttn:
            self.status.set("Заповніть BoxID та ТТН")
            return
        record = {"user_name": user_name, "boxid": boxid, "ttn": ttn}
        self.box_var.set("")
        self.ttn_var.set("")

        def task() -> Dict[str, Any]:
            if not token:
                raise ApiError("Відсутній токен", 401)
            return self.app.api.request_json(
                "POST", "/add_record", token=token, payload=record
            )

        def on_success(data: Dict[str, Any]) -> None:
            note = data.get("note") if isinstance(data, dict) else None
            if note:
                self.status.set(f"⚠️ Дублікат: {note}")
            else:
                self.status.set("✅ Успішно додано")
            self.sync_offline()

        def on_error(exc: Exception) -> None:
            self.app.tracking_offline.add(record)
            self.inflight.set(len(self.app.tracking_offline.list()))
            self.status.set("📦 Збережено локально (офлайн)")
            if not isinstance(exc, ApiError):
                return

        run_async(self, task, on_success, on_error)

    def sync_offline(self) -> None:
        token = self.app.state_data.get("token")
        if not token:
            return
        pending = self.app.tracking_offline.list()
        if not pending:
            self.status.set("Офлайн-черга порожня")
            return

        def task() -> int:
            synced = 0
            for record in pending:
                try:
                    self.app.api.request_json(
                        "POST", "/add_record", token=token, payload=record
                    )
                    synced += 1
                except ApiError:
                    break
            return synced

        def on_success(count: int) -> None:
            if count:
                self.app.tracking_offline.clear()
                self.status.set(f"Синхронізовано {count} записів")
            self.inflight.set(len(self.app.tracking_offline.list()))

        def on_error(_: Exception) -> None:
            self.status.set("Не вдалося синхронізувати")

        run_async(self, task, on_success, on_error)


class HistoryTab(ttk.Frame):
    def __init__(self, parent: ttk.Notebook, app: TrackingApp) -> None:
        super().__init__(parent, padding=20)
        self.app = app
        self.records: List[Dict[str, Any]] = []
        self.filtered: List[Dict[str, Any]] = []

        filters = ttk.LabelFrame(self, text="Фільтри", padding=12)
        filters.pack(fill=tk.X)

        self.box_filter = tk.StringVar()
        self.ttn_filter = tk.StringVar()
        self.user_filter = tk.StringVar()
        self.date_filter = tk.StringVar()
        self.start_time_filter = tk.StringVar()
        self.end_time_filter = tk.StringVar()

        self._filter_entry(filters, "BoxID", self.box_filter, 0)
        self._filter_entry(filters, "ТТН", self.ttn_filter, 1)
        self._filter_entry(filters, "Оператор", self.user_filter, 2)
        self._filter_entry(filters, "Дата (YYYY-MM-DD)", self.date_filter, 3)
        self._filter_entry(filters, "Час від (HH:MM)", self.start_time_filter, 4)
        self._filter_entry(filters, "Час до (HH:MM)", self.end_time_filter, 5)

        actions = ttk.Frame(filters)
        actions.grid(row=2, column=0, columnspan=6, sticky="w", pady=(10, 0))
        ttk.Button(
            actions,
            text="Застосувати",
            style="Secondary.TButton",
            command=self.apply_filters,
        ).pack(side=tk.LEFT)
        ttk.Button(
            actions,
            text="Очистити",
            style="Secondary.TButton",
            command=self.clear_filters,
        ).pack(side=tk.LEFT, padx=8)
        ttk.Button(
            actions,
            text="Оновити",
            style="Accent.TButton",
            command=self.fetch_history,
        ).pack(side=tk.LEFT, padx=8)
        ttk.Button(
            actions,
            text="Очистити історію",
            style="Secondary.TButton",
            command=self.clear_history,
        ).pack(side=tk.LEFT, padx=8)

        self.tree = ttk.Treeview(
            self,
            columns=("datetime", "user", "boxid", "ttn"),
            show="headings",
        )
        for col, label in [
            ("datetime", "Дата"),
            ("user", "Оператор"),
            ("boxid", "BoxID"),
            ("ttn", "ТТН"),
        ]:
            self.tree.heading(col, text=label)
            self.tree.column(col, width=180, anchor="center")
        self.tree.pack(fill=tk.BOTH, expand=True, pady=12)

    def _filter_entry(
        self, parent: ttk.LabelFrame, label: str, variable: tk.StringVar, col: int
    ) -> None:
        ttk.Label(parent, text=label).grid(row=0, column=col, sticky="w", padx=4)
        ttk.Entry(parent, textvariable=variable, width=18).grid(
            row=1, column=col, padx=4, pady=6
        )

    def refresh(self) -> None:
        self.fetch_history()

    def fetch_history(self) -> None:
        token = self.app.state_data.get("token")
        if not token:
            return

        def task() -> List[Dict[str, Any]]:
            data = self.app.api.request_json("GET", "/get_history", token=token)
            if isinstance(data, list):
                return data
            return []

        def on_success(data: List[Dict[str, Any]]) -> None:
            self.records = sorted(
                data,
                key=lambda item: item.get("datetime", ""),
                reverse=True,
            )
            self.apply_filters()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def apply_filters(self) -> None:
        filtered = list(self.records)
        if self.box_filter.get():
            filtered = [
                rec
                for rec in filtered
                if self.box_filter.get().strip() in str(rec.get("boxid", ""))
            ]
        if self.ttn_filter.get():
            filtered = [
                rec
                for rec in filtered
                if self.ttn_filter.get().strip() in str(rec.get("ttn", ""))
            ]
        if self.user_filter.get():
            token = self.user_filter.get().strip().lower()
            filtered = [
                rec
                for rec in filtered
                if token in str(rec.get("user_name", "")).lower()
            ]

        selected_date = parse_date(self.date_filter.get().strip())
        if selected_date:
            filtered = [
                rec
                for rec in filtered
                if self._match_date(rec.get("datetime"), selected_date)
            ]

        start_time = parse_time(self.start_time_filter.get().strip())
        end_time = parse_time(self.end_time_filter.get().strip())
        if start_time or end_time:
            filtered = [
                rec
                for rec in filtered
                if self._match_time(rec.get("datetime"), start_time, end_time)
            ]

        self.filtered = filtered
        self._refresh_tree()

    def _match_date(self, value: Any, selected: date) -> bool:
        try:
            parsed = datetime.fromisoformat(str(value)).date()
        except ValueError:
            return False
        return parsed == selected

    def _match_time(
        self, value: Any, start: Optional[time], end: Optional[time]
    ) -> bool:
        try:
            parsed = datetime.fromisoformat(str(value)).time()
        except ValueError:
            return False
        if start and parsed < start:
            return False
        if end and parsed > end:
            return False
        return True

    def _refresh_tree(self) -> None:
        self.tree.delete(*self.tree.get_children())
        for record in self.filtered:
            self.tree.insert(
                "",
                tk.END,
                values=(
                    format_datetime(record.get("datetime", "")),
                    record.get("user_name", ""),
                    record.get("boxid", ""),
                    record.get("ttn", ""),
                ),
            )

    def clear_filters(self) -> None:
        self.box_filter.set("")
        self.ttn_filter.set("")
        self.user_filter.set("")
        self.date_filter.set("")
        self.start_time_filter.set("")
        self.end_time_filter.set("")
        self.apply_filters()

    def clear_history(self) -> None:
        role = self.app.state_data.get("user_role")
        access_level = self.app.state_data.get("access_level")
        if role != "admin" and access_level != 1:
            messagebox.showinfo("Доступ", "Очистка доступна лише адміну")
            return
        token = self.app.state_data.get("token")
        if not token:
            return
        if not messagebox.askyesno("Підтвердження", "Очистити історію?"):
            return

        def task() -> Any:
            return self.app.api.request_json("DELETE", "/clear_tracking", token=token)

        def on_success(_: Any) -> None:
            self.records = []
            self.apply_filters()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)


class ErrorsTab(ttk.Frame):
    def __init__(self, parent: ttk.Notebook, app: TrackingApp) -> None:
        super().__init__(parent, padding=20)
        self.app = app
        self.records: List[Dict[str, Any]] = []

        actions = ttk.Frame(self)
        actions.pack(fill=tk.X)
        ttk.Button(
            actions,
            text="Оновити",
            style="Accent.TButton",
            command=self.fetch_errors,
        ).pack(side=tk.LEFT)
        ttk.Button(
            actions,
            text="Очистити всі",
            style="Secondary.TButton",
            command=self.clear_errors,
        ).pack(side=tk.LEFT, padx=8)
        ttk.Button(
            actions,
            text="Видалити вибране",
            style="Secondary.TButton",
            command=self.delete_selected,
        ).pack(side=tk.LEFT)

        self.tree = ttk.Treeview(
            self,
            columns=("datetime", "user", "boxid", "ttn", "note", "id"),
            show="headings",
        )
        for col, label in [
            ("datetime", "Дата"),
            ("user", "Оператор"),
            ("boxid", "BoxID"),
            ("ttn", "ТТН"),
            ("note", "Примітка"),
            ("id", "ID"),
        ]:
            self.tree.heading(col, text=label)
            self.tree.column(col, width=150, anchor="center")
        self.tree.pack(fill=tk.BOTH, expand=True, pady=12)

    def refresh(self) -> None:
        self.fetch_errors()

    def fetch_errors(self) -> None:
        token = self.app.state_data.get("token")
        if not token:
            return

        def task() -> List[Dict[str, Any]]:
            data = self.app.api.request_json("GET", "/get_errors", token=token)
            return data if isinstance(data, list) else []

        def on_success(data: List[Dict[str, Any]]) -> None:
            self.records = data
            self.tree.delete(*self.tree.get_children())
            for record in data:
                self.tree.insert(
                    "",
                    tk.END,
                    values=(
                        format_datetime(record.get("datetime", "")),
                        record.get("user_name", ""),
                        record.get("boxid", ""),
                        record.get("ttn", ""),
                        record.get("note", ""),
                        record.get("id", ""),
                    ),
                )

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def clear_errors(self) -> None:
        role = self.app.state_data.get("user_role")
        access_level = self.app.state_data.get("access_level")
        if role not in {"admin", "operator"} and access_level not in {0, 1}:
            messagebox.showinfo("Доступ", "Недостатньо прав")
            return
        token = self.app.state_data.get("token")
        if not token:
            return
        if not messagebox.askyesno("Підтвердження", "Очистити всі помилки?"):
            return

        def task() -> Any:
            return self.app.api.request_json("DELETE", "/clear_errors", token=token)

        def on_success(_: Any) -> None:
            self.records = []
            self.tree.delete(*self.tree.get_children())

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def delete_selected(self) -> None:
        selection = self.tree.selection()
        if not selection:
            messagebox.showinfo("Помилка", "Оберіть запис для видалення")
            return
        item = self.tree.item(selection[0])
        record_id = item["values"][5]
        token = self.app.state_data.get("token")
        if not token:
            return

        def task() -> Any:
            return self.app.api.request_json(
                "DELETE", f"/delete_error/{record_id}", token=token
            )

        def on_success(_: Any) -> None:
            self.fetch_errors()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)


class StatisticsTab(ttk.Frame):
    def __init__(self, parent: ttk.Notebook, app: TrackingApp) -> None:
        super().__init__(parent, padding=20)
        self.app = app
        self.history: List[Dict[str, Any]] = []
        self.errors: List[Dict[str, Any]] = []
        self.summary = tk.StringVar(value="")
        self.start_date = tk.StringVar()
        self.end_date = tk.StringVar()

        filters = ttk.Frame(self)
        filters.pack(fill=tk.X)
        ttk.Label(filters, text="Початок (YYYY-MM-DD)").pack(side=tk.LEFT)
        ttk.Entry(filters, textvariable=self.start_date, width=12).pack(
            side=tk.LEFT, padx=6
        )
        ttk.Label(filters, text="Кінець (YYYY-MM-DD)").pack(side=tk.LEFT)
        ttk.Entry(filters, textvariable=self.end_date, width=12).pack(
            side=tk.LEFT, padx=6
        )
        ttk.Button(
            filters,
            text="Оновити",
            style="Accent.TButton",
            command=self.fetch_data,
        ).pack(side=tk.LEFT, padx=8)

        ttk.Label(self, textvariable=self.summary, style="Status.TLabel").pack(
            pady=12
        )

        self.tree = ttk.Treeview(
            self,
            columns=("user", "scans", "errors"),
            show="headings",
            height=10,
        )
        for col, label in [("user", "Оператор"), ("scans", "Сканів"), ("errors", "Помилок")]:
            self.tree.heading(col, text=label)
            self.tree.column(col, width=180, anchor="center")
        self.tree.pack(fill=tk.BOTH, expand=True)

    def refresh(self) -> None:
        role = self.app.state_data.get("user_role")
        access_level = self.app.state_data.get("access_level")
        if role != "admin" and access_level != 1:
            self.summary.set("Доступ до статистики лише для адміністраторів")
            return
        if not self.start_date.get() or not self.end_date.get():
            today = datetime.now().date()
            self.start_date.set(today.replace(day=1).isoformat())
            self.end_date.set(today.isoformat())
        self.fetch_data()

    def fetch_data(self) -> None:
        token = self.app.state_data.get("token")
        if not token:
            return

        def task() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
            history = self.app.api.request_json("GET", "/get_history", token=token)
            errors = self.app.api.request_json("GET", "/get_errors", token=token)
            return (
                history if isinstance(history, list) else [],
                errors if isinstance(errors, list) else [],
            )

        def on_success(data: Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]) -> None:
            self.history, self.errors = data
            self.apply_stats()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def apply_stats(self) -> None:
        start = parse_date(self.start_date.get().strip())
        end = parse_date(self.end_date.get().strip())
        if not start or not end:
            messagebox.showinfo("Фільтр", "Вкажіть коректні дати")
            return

        def in_range(record: Dict[str, Any]) -> bool:
            try:
                dt = datetime.fromisoformat(record.get("datetime", "")).date()
            except ValueError:
                return False
            return start <= dt <= end

        history = [rec for rec in self.history if in_range(rec)]
        errors = [rec for rec in self.errors if in_range(rec)]

        counts: Dict[str, int] = {}
        error_counts: Dict[str, int] = {}
        for rec in history:
            user = rec.get("user_name", "—")
            counts[user] = counts.get(user, 0) + 1
        for rec in errors:
            user = rec.get("user_name", "—")
            error_counts[user] = error_counts.get(user, 0) + 1

        total_scans = sum(counts.values())
        total_errors = sum(error_counts.values())
        top_user = max(counts.items(), key=lambda item: item[1], default=("—", 0))
        self.summary.set(
            f"Сканів: {total_scans} | Помилок: {total_errors} | Топ оператор: {top_user[0]}"
        )

        self.tree.delete(*self.tree.get_children())
        for user, scans in sorted(counts.items(), key=lambda item: item[1], reverse=True):
            self.tree.insert(
                "",
                tk.END,
                values=(user, scans, error_counts.get(user, 0)),
            )


class ScanpakMainFrame(ttk.Frame):
    def __init__(self, parent: ttk.Frame, app: TrackingApp) -> None:
        super().__init__(parent)
        self.app = app
        self.status = tk.StringVar(value="")
        self.user_label = tk.StringVar(value="")

        header = ttk.Frame(self)
        header.pack(fill=tk.X)
        ttk.Label(header, text="СканПак", style="Header.TLabel").pack(side=tk.LEFT)
        ttk.Label(header, textvariable=self.user_label).pack(side=tk.LEFT, padx=20)
        ttk.Button(
            header,
            text="Вийти",
            style="Secondary.TButton",
            command=self.logout,
        ).pack(side=tk.RIGHT)

        self.tabs = ttk.Notebook(self)
        self.tabs.pack(fill=tk.BOTH, expand=True, pady=12)

        self.scan_tab = ScanpakScanTab(self.tabs, app, self.status)
        self.history_tab = ScanpakHistoryTab(self.tabs, app)
        self.stats_tab = ScanpakStatsTab(self.tabs, app)
        self.tabs.add(self.scan_tab, text="Сканування")
        self.tabs.add(self.history_tab, text="Історія")
        self.tabs.add(self.stats_tab, text="Статистика")

        ttk.Label(self, textvariable=self.status, style="Status.TLabel").pack(
            pady=(0, 8)
        )

    def refresh(self) -> None:
        name = self.app.state_data.get("scanpak_user_name", "оператор")
        role = self.app.state_data.get("scanpak_user_role", "")
        role_label = "Адмін" if role == "admin" else "Оператор"
        self.user_label.set(f"{name} • {role_label}")
        self.scan_tab.refresh()
        self.history_tab.refresh()
        self.stats_tab.refresh()

    def logout(self) -> None:
        self.app.clear_state(["scanpak_token", "scanpak_user_name", "scanpak_user_role"])
        self.app.show_frame("StartFrame")


class ScanpakScanTab(ttk.Frame):
    def __init__(self, parent: ttk.Notebook, app: TrackingApp, status: tk.StringVar) -> None:
        super().__init__(parent, padding=20)
        self.app = app
        self.status = status
        self.number_var = tk.StringVar()
        self.offline_count = tk.IntVar(value=0)

        ttk.Label(self, text="Номер відправлення").pack(anchor="w")
        entry = ttk.Entry(self, textvariable=self.number_var)
        entry.pack(fill=tk.X, pady=(0, 12))
        entry.bind("<Return>", lambda _: self.submit())

        actions = ttk.Frame(self)
        actions.pack(fill=tk.X)
        ttk.Button(
            actions,
            text="Зберегти",
            style="Accent.TButton",
            command=self.submit,
        ).pack(side=tk.LEFT)
        ttk.Button(
            actions,
            text="Синхронізувати",
            style="Secondary.TButton",
            command=self.sync_offline,
        ).pack(side=tk.LEFT, padx=8)
        ttk.Label(actions, textvariable=self.offline_count).pack(side=tk.RIGHT)

    def refresh(self) -> None:
        self.offline_count.set(len(self.app.scanpak_offline.list()))

    def submit(self) -> None:
        token = self.app.state_data.get("scanpak_token")
        number = "".join(filter(str.isdigit, self.number_var.get()))
        if not number:
            self.status.set("Введіть номер")
            return
        if self.app.scanpak_offline.contains("parcel_number", number):
            self.status.set("Увага, це дублікат в офлайн черзі")
            self.number_var.set("")
            return
        self.number_var.set("")

        def task() -> Dict[str, Any]:
            if not token:
                raise ApiError("Відсутній токен", 401)
            return self.app.scanpak_api.request_json(
                "POST", "/scans", token=token, payload={"parcel_number": number}
            )

        def on_success(_: Dict[str, Any]) -> None:
            self.status.set("✅ Збережено")
            self.sync_offline()

        def on_error(exc: Exception) -> None:
            self.app.scanpak_offline.add({"parcel_number": number})
            self.offline_count.set(len(self.app.scanpak_offline.list()))
            if isinstance(exc, ApiError):
                self.status.set(exc.message)
            else:
                self.status.set("Збережено локально (офлайн)")

        run_async(self, task, on_success, on_error)

    def sync_offline(self) -> None:
        token = self.app.state_data.get("scanpak_token")
        if not token:
            return
        pending = self.app.scanpak_offline.list()
        if not pending:
            self.status.set("Офлайн-черга порожня")
            return

        def task() -> int:
            synced = 0
            for record in pending:
                try:
                    self.app.scanpak_api.request_json(
                        "POST", "/scans", token=token, payload=record
                    )
                    synced += 1
                except ApiError:
                    break
            return synced

        def on_success(count: int) -> None:
            if count:
                self.app.scanpak_offline.clear()
            self.offline_count.set(len(self.app.scanpak_offline.list()))
            self.status.set(f"Синхронізовано {count} записів")

        def on_error(_: Exception) -> None:
            self.status.set("Не вдалося синхронізувати")

        run_async(self, task, on_success, on_error)


class ScanpakHistoryTab(ttk.Frame):
    def __init__(self, parent: ttk.Notebook, app: TrackingApp) -> None:
        super().__init__(parent, padding=20)
        self.app = app
        self.records: List[Dict[str, Any]] = []
        self.filtered: List[Dict[str, Any]] = []

        filters = ttk.LabelFrame(self, text="Фільтри", padding=12)
        filters.pack(fill=tk.X)
        self.parcel_filter = tk.StringVar()
        self.user_filter = tk.StringVar()
        self.date_filter = tk.StringVar()

        ttk.Label(filters, text="Відправлення").grid(row=0, column=0, sticky="w")
        ttk.Entry(filters, textvariable=self.parcel_filter, width=16).grid(
            row=1, column=0, padx=4
        )
        ttk.Label(filters, text="Оператор").grid(row=0, column=1, sticky="w")
        ttk.Entry(filters, textvariable=self.user_filter, width=16).grid(
            row=1, column=1, padx=4
        )
        ttk.Label(filters, text="Дата (YYYY-MM-DD)").grid(
            row=0, column=2, sticky="w"
        )
        ttk.Entry(filters, textvariable=self.date_filter, width=16).grid(
            row=1, column=2, padx=4
        )

        actions = ttk.Frame(filters)
        actions.grid(row=2, column=0, columnspan=3, sticky="w", pady=(10, 0))
        ttk.Button(
            actions,
            text="Застосувати",
            style="Secondary.TButton",
            command=self.apply_filters,
        ).pack(side=tk.LEFT)
        ttk.Button(
            actions,
            text="Очистити",
            style="Secondary.TButton",
            command=self.clear_filters,
        ).pack(side=tk.LEFT, padx=8)
        ttk.Button(
            actions,
            text="Оновити",
            style="Accent.TButton",
            command=self.fetch_history,
        ).pack(side=tk.LEFT, padx=8)

        self.tree = ttk.Treeview(
            self,
            columns=("datetime", "user", "parcel"),
            show="headings",
        )
        for col, label in [
            ("datetime", "Дата"),
            ("user", "Оператор"),
            ("parcel", "Номер"),
        ]:
            self.tree.heading(col, text=label)
            self.tree.column(col, width=200, anchor="center")
        self.tree.pack(fill=tk.BOTH, expand=True, pady=12)

    def refresh(self) -> None:
        self.fetch_history()

    def fetch_history(self) -> None:
        token = self.app.state_data.get("scanpak_token")
        if not token:
            return

        def task() -> List[Dict[str, Any]]:
            data = self.app.scanpak_api.request_json("GET", "/history", token=token)
            return data if isinstance(data, list) else []

        def on_success(data: List[Dict[str, Any]]) -> None:
            self.records = data
            self.apply_filters()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def apply_filters(self) -> None:
        filtered = list(self.records)
        if self.parcel_filter.get():
            filtered = [
                rec
                for rec in filtered
                if self.parcel_filter.get().strip() in str(rec.get("parcel_number", ""))
            ]
        if self.user_filter.get():
            token = self.user_filter.get().strip().lower()
            filtered = [
                rec
                for rec in filtered
                if token in str(rec.get("user", rec.get("user_name", ""))).lower()
            ]
        selected_date = parse_date(self.date_filter.get().strip())
        if selected_date:
            filtered = [
                rec
                for rec in filtered
                if self._match_date(rec.get("timestamp", rec.get("datetime")), selected_date)
            ]
        self.filtered = filtered
        self.tree.delete(*self.tree.get_children())
        for record in filtered:
            timestamp = record.get("timestamp", record.get("datetime", ""))
            self.tree.insert(
                "",
                tk.END,
                values=(
                    format_datetime(str(timestamp)),
                    record.get("user", record.get("user_name", "")),
                    record.get("parcel_number", ""),
                ),
            )

    def _match_date(self, value: Any, selected: date) -> bool:
        try:
            parsed = datetime.fromisoformat(str(value)).date()
        except ValueError:
            return False
        return parsed == selected

    def clear_filters(self) -> None:
        self.parcel_filter.set("")
        self.user_filter.set("")
        self.date_filter.set("")
        self.apply_filters()


class ScanpakStatsTab(ttk.Frame):
    def __init__(self, parent: ttk.Notebook, app: TrackingApp) -> None:
        super().__init__(parent, padding=20)
        self.app = app
        self.summary = tk.StringVar()
        self.start_date = tk.StringVar()
        self.end_date = tk.StringVar()
        self.records: List[Dict[str, Any]] = []

        filters = ttk.Frame(self)
        filters.pack(fill=tk.X)
        ttk.Label(filters, text="Початок (YYYY-MM-DD)").pack(side=tk.LEFT)
        ttk.Entry(filters, textvariable=self.start_date, width=12).pack(
            side=tk.LEFT, padx=6
        )
        ttk.Label(filters, text="Кінець (YYYY-MM-DD)").pack(side=tk.LEFT)
        ttk.Entry(filters, textvariable=self.end_date, width=12).pack(
            side=tk.LEFT, padx=6
        )
        ttk.Button(
            filters,
            text="Оновити",
            style="Accent.TButton",
            command=self.fetch_data,
        ).pack(side=tk.LEFT, padx=8)

        ttk.Label(self, textvariable=self.summary, style="Status.TLabel").pack(
            pady=12
        )

        self.tree = ttk.Treeview(
            self,
            columns=("user", "count"),
            show="headings",
            height=10,
        )
        self.tree.heading("user", text="Оператор")
        self.tree.heading("count", text="Сканів")
        self.tree.column("user", width=200, anchor="center")
        self.tree.column("count", width=100, anchor="center")
        self.tree.pack(fill=tk.BOTH, expand=True)

    def refresh(self) -> None:
        if not self.start_date.get() or not self.end_date.get():
            today = datetime.now().date()
            self.start_date.set(today.replace(day=1).isoformat())
            self.end_date.set(today.isoformat())
        self.fetch_data()

    def fetch_data(self) -> None:
        token = self.app.state_data.get("scanpak_token")
        if not token:
            return

        def task() -> List[Dict[str, Any]]:
            data = self.app.scanpak_api.request_json("GET", "/history", token=token)
            return data if isinstance(data, list) else []

        def on_success(data: List[Dict[str, Any]]) -> None:
            self.records = data
            self.apply_stats()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def apply_stats(self) -> None:
        start = parse_date(self.start_date.get().strip())
        end = parse_date(self.end_date.get().strip())
        if not start or not end:
            messagebox.showinfo("Фільтр", "Вкажіть коректні дати")
            return
        counts: Dict[str, int] = {}
        for record in self.records:
            timestamp = record.get("timestamp", record.get("datetime", ""))
            try:
                rec_date = datetime.fromisoformat(str(timestamp)).date()
            except ValueError:
                continue
            if start <= rec_date <= end:
                user = record.get("user", record.get("user_name", "—"))
                counts[user] = counts.get(user, 0) + 1
        total = sum(counts.values())
        top = max(counts.items(), key=lambda item: item[1], default=("—", 0))
        self.summary.set(f"Сканів: {total} | Топ оператор: {top[0]}")
        self.tree.delete(*self.tree.get_children())
        for user, count in sorted(counts.items(), key=lambda item: item[1], reverse=True):
            self.tree.insert("", tk.END, values=(user, count))


class AdminPanel(tk.Toplevel):
    def __init__(self, parent: tk.Misc, app: TrackingApp, token: str) -> None:
        super().__init__(parent)
        self.app = app
        self.token = token
        self.title("Адмін панель")
        self.geometry("900x600")

        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True)

        self.pending_tab = AdminPendingTab(notebook, app, token)
        self.users_tab = AdminUsersTab(notebook, app, token)
        self.password_tab = AdminPasswordsTab(notebook, app, token)
        notebook.add(self.pending_tab, text="Запити")
        notebook.add(self.users_tab, text="Користувачі")
        notebook.add(self.password_tab, text="Паролі ролей")


class AdminPendingTab(ttk.Frame):
    def __init__(self, parent: ttk.Notebook, app: TrackingApp, token: str) -> None:
        super().__init__(parent, padding=20)
        self.app = app
        self.token = token
        self.requests: List[Dict[str, Any]] = []

        actions = ttk.Frame(self)
        actions.pack(fill=tk.X)
        ttk.Button(
            actions,
            text="Оновити",
            style="Accent.TButton",
            command=self.fetch_requests,
        ).pack(side=tk.LEFT)
        ttk.Button(
            actions,
            text="Підтвердити",
            style="Secondary.TButton",
            command=self.approve_request,
        ).pack(side=tk.LEFT, padx=8)
        ttk.Button(
            actions,
            text="Відхилити",
            style="Secondary.TButton",
            command=self.reject_request,
        ).pack(side=tk.LEFT)

        self.role_var = tk.StringVar(value="operator")
        ttk.Label(actions, text="Роль").pack(side=tk.LEFT, padx=8)
        ttk.Combobox(
            actions, textvariable=self.role_var, values=["admin", "operator", "viewer"], width=12
        ).pack(side=tk.LEFT)

        self.tree = ttk.Treeview(
            self, columns=("id", "surname", "created"), show="headings"
        )
        for col, label in [("id", "ID"), ("surname", "Прізвище"), ("created", "Дата")]:
            self.tree.heading(col, text=label)
            self.tree.column(col, width=140, anchor="center")
        self.tree.pack(fill=tk.BOTH, expand=True, pady=12)
        self.fetch_requests()

    def fetch_requests(self) -> None:
        def task() -> List[Dict[str, Any]]:
            data = self.app.api.request_json(
                "GET", "/admin/registration_requests", token=self.token
            )
            return data if isinstance(data, list) else []

        def on_success(data: List[Dict[str, Any]]) -> None:
            self.requests = data
            self.tree.delete(*self.tree.get_children())
            for req in data:
                self.tree.insert(
                    "",
                    tk.END,
                    values=(
                        req.get("id"),
                        req.get("surname", ""),
                        format_datetime(req.get("created_at", "")),
                    ),
                )

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def _selected_id(self) -> Optional[int]:
        selection = self.tree.selection()
        if not selection:
            return None
        return int(self.tree.item(selection[0])["values"][0])

    def approve_request(self) -> None:
        request_id = self._selected_id()
        if request_id is None:
            messagebox.showinfo("Помилка", "Оберіть запит")
            return
        role = self.role_var.get()

        def task() -> Any:
            return self.app.api.request_json(
                "POST",
                f"/admin/registration_requests/{request_id}/approve",
                token=self.token,
                payload={"role": role},
            )

        def on_success(_: Any) -> None:
            self.fetch_requests()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def reject_request(self) -> None:
        request_id = self._selected_id()
        if request_id is None:
            messagebox.showinfo("Помилка", "Оберіть запит")
            return

        def task() -> Any:
            return self.app.api.request_json(
                "POST",
                f"/admin/registration_requests/{request_id}/reject",
                token=self.token,
            )

        def on_success(_: Any) -> None:
            self.fetch_requests()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)


class AdminUsersTab(ttk.Frame):
    def __init__(self, parent: ttk.Notebook, app: TrackingApp, token: str) -> None:
        super().__init__(parent, padding=20)
        self.app = app
        self.token = token

        actions = ttk.Frame(self)
        actions.pack(fill=tk.X)
        ttk.Button(
            actions,
            text="Оновити",
            style="Accent.TButton",
            command=self.fetch_users,
        ).pack(side=tk.LEFT)
        ttk.Button(
            actions,
            text="Змінити роль",
            style="Secondary.TButton",
            command=self.change_role,
        ).pack(side=tk.LEFT, padx=8)
        ttk.Button(
            actions,
            text="Активність",
            style="Secondary.TButton",
            command=self.toggle_active,
        ).pack(side=tk.LEFT)

        self.role_var = tk.StringVar(value="operator")
        self.active_var = tk.StringVar(value="true")
        ttk.Label(actions, text="Роль").pack(side=tk.LEFT, padx=8)
        ttk.Combobox(
            actions, textvariable=self.role_var, values=["admin", "operator", "viewer"], width=12
        ).pack(side=tk.LEFT)
        ttk.Label(actions, text="Активність").pack(side=tk.LEFT, padx=8)
        ttk.Combobox(actions, textvariable=self.active_var, values=["true", "false"], width=8).pack(
            side=tk.LEFT
        )

        self.tree = ttk.Treeview(
            self,
            columns=("id", "surname", "role", "active"),
            show="headings",
        )
        for col, label in [
            ("id", "ID"),
            ("surname", "Прізвище"),
            ("role", "Роль"),
            ("active", "Активний"),
        ]:
            self.tree.heading(col, text=label)
            self.tree.column(col, width=150, anchor="center")
        self.tree.pack(fill=tk.BOTH, expand=True, pady=12)
        self.fetch_users()

    def fetch_users(self) -> None:
        def task() -> List[Dict[str, Any]]:
            data = self.app.api.request_json("GET", "/admin/users", token=self.token)
            return data if isinstance(data, list) else []

        def on_success(data: List[Dict[str, Any]]) -> None:
            self.tree.delete(*self.tree.get_children())
            for user in data:
                self.tree.insert(
                    "",
                    tk.END,
                    values=(
                        user.get("id"),
                        user.get("surname"),
                        user.get("role"),
                        "Так" if user.get("is_active", False) else "Ні",
                    ),
                )

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def _selected_id(self) -> Optional[int]:
        selection = self.tree.selection()
        if not selection:
            return None
        return int(self.tree.item(selection[0])["values"][0])

    def change_role(self) -> None:
        user_id = self._selected_id()
        if user_id is None:
            messagebox.showinfo("Помилка", "Оберіть користувача")
            return
        role = self.role_var.get()

        def task() -> Any:
            return self.app.api.request_json(
                "PATCH", f"/admin/users/{user_id}", token=self.token, payload={"role": role}
            )

        def on_success(_: Any) -> None:
            self.fetch_users()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def toggle_active(self) -> None:
        user_id = self._selected_id()
        if user_id is None:
            messagebox.showinfo("Помилка", "Оберіть користувача")
            return
        is_active = self.active_var.get().lower() == "true"

        def task() -> Any:
            return self.app.api.request_json(
                "PATCH",
                f"/admin/users/{user_id}",
                token=self.token,
                payload={"is_active": is_active},
            )

        def on_success(_: Any) -> None:
            self.fetch_users()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)


class AdminPasswordsTab(ttk.Frame):
    def __init__(self, parent: ttk.Notebook, app: TrackingApp, token: str) -> None:
        super().__init__(parent, padding=20)
        self.app = app
        self.token = token
        self.passwords: Dict[str, str] = {}

        actions = ttk.Frame(self)
        actions.pack(fill=tk.X)
        ttk.Button(
            actions,
            text="Оновити",
            style="Accent.TButton",
            command=self.fetch_passwords,
        ).pack(side=tk.LEFT)
        ttk.Button(
            actions,
            text="Зберегти",
            style="Secondary.TButton",
            command=self.update_password,
        ).pack(side=tk.LEFT, padx=8)

        self.role_var = tk.StringVar(value="operator")
        self.password_var = tk.StringVar()
        ttk.Label(actions, text="Роль").pack(side=tk.LEFT, padx=8)
        ttk.Combobox(
            actions, textvariable=self.role_var, values=["admin", "operator", "viewer"], width=12
        ).pack(side=tk.LEFT)
        ttk.Entry(actions, textvariable=self.password_var, width=24, show="*").pack(
            side=tk.LEFT, padx=8
        )
        self.fetch_passwords()

    def fetch_passwords(self) -> None:
        def task() -> Dict[str, Any]:
            data = self.app.api.request_json(
                "GET", "/admin/role-passwords", token=self.token
            )
            return data if isinstance(data, dict) else {}

        def on_success(data: Dict[str, Any]) -> None:
            self.passwords = {str(k): str(v) for k, v in data.items()}
            role = self.role_var.get()
            self.password_var.set(self.passwords.get(role, ""))

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def update_password(self) -> None:
        role = self.role_var.get()
        password = self.password_var.get().strip()
        if not password:
            messagebox.showinfo("Помилка", "Введіть пароль")
            return

        def task() -> Any:
            return self.app.api.request_json(
                "POST",
                f"/admin/role-passwords/{role}",
                token=self.token,
                payload={"password": password},
            )

        def on_success(_: Any) -> None:
            self.fetch_passwords()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)


class ScanpakAdminPanel(tk.Toplevel):
    def __init__(self, parent: tk.Misc, app: TrackingApp, token: str) -> None:
        super().__init__(parent)
        self.app = app
        self.token = token
        self.title("Адмін панель Scanpak")
        self.geometry("900x600")

        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True)

        self.pending_tab = ScanpakAdminPendingTab(notebook, app, token)
        self.users_tab = ScanpakAdminUsersTab(notebook, app, token)
        notebook.add(self.pending_tab, text="Запити")
        notebook.add(self.users_tab, text="Користувачі")


class ScanpakAdminPendingTab(ttk.Frame):
    def __init__(self, parent: ttk.Notebook, app: TrackingApp, token: str) -> None:
        super().__init__(parent, padding=20)
        self.app = app
        self.token = token

        actions = ttk.Frame(self)
        actions.pack(fill=tk.X)
        ttk.Button(
            actions,
            text="Оновити",
            style="Accent.TButton",
            command=self.fetch_requests,
        ).pack(side=tk.LEFT)
        ttk.Button(
            actions,
            text="Підтвердити",
            style="Secondary.TButton",
            command=self.approve_request,
        ).pack(side=tk.LEFT, padx=8)
        ttk.Button(
            actions,
            text="Відхилити",
            style="Secondary.TButton",
            command=self.reject_request,
        ).pack(side=tk.LEFT)

        self.role_var = tk.StringVar(value="operator")
        ttk.Label(actions, text="Роль").pack(side=tk.LEFT, padx=8)
        ttk.Combobox(
            actions, textvariable=self.role_var, values=["admin", "operator"], width=12
        ).pack(side=tk.LEFT)

        self.tree = ttk.Treeview(
            self, columns=("id", "surname", "created"), show="headings"
        )
        for col, label in [("id", "ID"), ("surname", "Прізвище"), ("created", "Дата")]:
            self.tree.heading(col, text=label)
            self.tree.column(col, width=140, anchor="center")
        self.tree.pack(fill=tk.BOTH, expand=True, pady=12)
        self.fetch_requests()

    def fetch_requests(self) -> None:
        def task() -> List[Dict[str, Any]]:
            data = self.app.scanpak_api.request_json(
                "GET", "/admin/registration_requests", token=self.token
            )
            return data if isinstance(data, list) else []

        def on_success(data: List[Dict[str, Any]]) -> None:
            self.tree.delete(*self.tree.get_children())
            for req in data:
                self.tree.insert(
                    "",
                    tk.END,
                    values=(
                        req.get("id"),
                        req.get("surname", ""),
                        format_datetime(req.get("created_at", "")),
                    ),
                )

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def _selected_id(self) -> Optional[int]:
        selection = self.tree.selection()
        if not selection:
            return None
        return int(self.tree.item(selection[0])["values"][0])

    def approve_request(self) -> None:
        request_id = self._selected_id()
        if request_id is None:
            messagebox.showinfo("Помилка", "Оберіть запит")
            return
        role = self.role_var.get()

        def task() -> Any:
            return self.app.scanpak_api.request_json(
                "POST",
                f"/admin/registration_requests/{request_id}/approve",
                token=self.token,
                payload={"role": role},
            )

        def on_success(_: Any) -> None:
            self.fetch_requests()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def reject_request(self) -> None:
        request_id = self._selected_id()
        if request_id is None:
            messagebox.showinfo("Помилка", "Оберіть запит")
            return

        def task() -> Any:
            return self.app.scanpak_api.request_json(
                "POST",
                f"/admin/registration_requests/{request_id}/reject",
                token=self.token,
            )

        def on_success(_: Any) -> None:
            self.fetch_requests()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)


class ScanpakAdminUsersTab(ttk.Frame):
    def __init__(self, parent: ttk.Notebook, app: TrackingApp, token: str) -> None:
        super().__init__(parent, padding=20)
        self.app = app
        self.token = token

        actions = ttk.Frame(self)
        actions.pack(fill=tk.X)
        ttk.Button(
            actions,
            text="Оновити",
            style="Accent.TButton",
            command=self.fetch_users,
        ).pack(side=tk.LEFT)
        ttk.Button(
            actions,
            text="Змінити роль",
            style="Secondary.TButton",
            command=self.change_role,
        ).pack(side=tk.LEFT, padx=8)
        ttk.Button(
            actions,
            text="Активність",
            style="Secondary.TButton",
            command=self.toggle_active,
        ).pack(side=tk.LEFT)

        self.role_var = tk.StringVar(value="operator")
        self.active_var = tk.StringVar(value="true")
        ttk.Label(actions, text="Роль").pack(side=tk.LEFT, padx=8)
        ttk.Combobox(
            actions, textvariable=self.role_var, values=["admin", "operator"], width=12
        ).pack(side=tk.LEFT)
        ttk.Label(actions, text="Активність").pack(side=tk.LEFT, padx=8)
        ttk.Combobox(actions, textvariable=self.active_var, values=["true", "false"], width=8).pack(
            side=tk.LEFT
        )

        self.tree = ttk.Treeview(
            self,
            columns=("id", "surname", "role", "active"),
            show="headings",
        )
        for col, label in [
            ("id", "ID"),
            ("surname", "Прізвище"),
            ("role", "Роль"),
            ("active", "Активний"),
        ]:
            self.tree.heading(col, text=label)
            self.tree.column(col, width=150, anchor="center")
        self.tree.pack(fill=tk.BOTH, expand=True, pady=12)
        self.fetch_users()

    def fetch_users(self) -> None:
        def task() -> List[Dict[str, Any]]:
            data = self.app.scanpak_api.request_json("GET", "/admin/users", token=self.token)
            return data if isinstance(data, list) else []

        def on_success(data: List[Dict[str, Any]]) -> None:
            self.tree.delete(*self.tree.get_children())
            for user in data:
                self.tree.insert(
                    "",
                    tk.END,
                    values=(
                        user.get("id"),
                        user.get("surname"),
                        user.get("role"),
                        "Так" if user.get("is_active", False) else "Ні",
                    ),
                )

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def _selected_id(self) -> Optional[int]:
        selection = self.tree.selection()
        if not selection:
            return None
        return int(self.tree.item(selection[0])["values"][0])

    def change_role(self) -> None:
        user_id = self._selected_id()
        if user_id is None:
            messagebox.showinfo("Помилка", "Оберіть користувача")
            return
        role = self.role_var.get()

        def task() -> Any:
            return self.app.scanpak_api.request_json(
                "PATCH", f"/admin/users/{user_id}", token=self.token, payload={"role": role}
            )

        def on_success(_: Any) -> None:
            self.fetch_users()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)

    def toggle_active(self) -> None:
        user_id = self._selected_id()
        if user_id is None:
            messagebox.showinfo("Помилка", "Оберіть користувача")
            return
        is_active = self.active_var.get().lower() == "true"

        def task() -> Any:
            return self.app.scanpak_api.request_json(
                "PATCH",
                f"/admin/users/{user_id}",
                token=self.token,
                payload={"is_active": is_active},
            )

        def on_success(_: Any) -> None:
            self.fetch_users()

        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))

        run_async(self, task, on_success, on_error)


def simple_prompt(root: tk.Misc, prompt: str) -> Optional[str]:
    dialog = tk.Toplevel(root)
    dialog.title("Ввід")
    dialog.grab_set()
    ttk.Label(dialog, text=prompt).pack(padx=20, pady=(20, 6))
    value = tk.StringVar()
    entry = ttk.Entry(dialog, textvariable=value, show="*")
    entry.pack(padx=20, pady=6, fill=tk.X)
    entry.focus()

    result: List[Optional[str]] = [None]

    def submit() -> None:
        result[0] = value.get().strip()
        dialog.destroy()

    ttk.Button(dialog, text="OK", style="Accent.TButton", command=submit).pack(
        pady=(6, 20)
    )
    dialog.wait_window()
    return result[0]


if __name__ == "__main__":
    app = TrackingApp()
    app.mainloop()
