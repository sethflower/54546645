import json
import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import flet as ft
import requests
from platformdirs import user_data_dir

APP_NAME = "TrackingApp"
APP_VENDOR = "TrackingApp"

TRACKING_API_HOST = "173.242.53.38"
TRACKING_API_PORT = 10000

SCANPAK_API_HOST = "173.242.53.38"
SCANPAK_API_PORT = 10000
SCANPAK_BASE_PATH = "/scanpak"


def api_base_url() -> str:
    return f"http://{TRACKING_API_HOST}:{TRACKING_API_PORT}"


def scanpak_base_url() -> str:
    return f"http://{SCANPAK_API_HOST}:{SCANPAK_API_PORT}{SCANPAK_BASE_PATH}"


class Settings:
    def __init__(self) -> None:
        self._path = os.path.join(user_data_dir(APP_NAME, APP_VENDOR), "settings.json")
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        self._data = self._load()

    def _load(self) -> Dict[str, Any]:
        if not os.path.exists(self._path):
            return {}
        try:
            with open(self._path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except (json.JSONDecodeError, OSError):
            return {}

    def save(self) -> None:
        with open(self._path, "w", encoding="utf-8") as handle:
            json.dump(self._data, handle, ensure_ascii=False, indent=2)

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value
        self.save()

    def remove(self, key: str) -> None:
        if key in self._data:
            del self._data[key]
            self.save()

    def clear(self, keys: List[str]) -> None:
        for key in keys:
            self._data.pop(key, None)
        self.save()


class OfflineStore:
    def __init__(self) -> None:
        self._path = os.path.join(user_data_dir(APP_NAME, APP_VENDOR), "offline_queue.db")
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._path)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS offline_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_name TEXT NOT NULL,
                    boxid TEXT NOT NULL,
                    ttn TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scanpak_offline_scans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parcel_number TEXT NOT NULL,
                    stored_at TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def add_tracking_record(self, record: Dict[str, str]) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO offline_records (user_name, boxid, ttn, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    record["user_name"],
                    record["boxid"],
                    record["ttn"],
                    datetime.utcnow().isoformat(),
                ),
            )
            conn.commit()

    def list_tracking_records(self) -> List[Dict[str, str]]:
        with self._connect() as conn:
            cursor = conn.execute(
                "SELECT id, user_name, boxid, ttn, created_at FROM offline_records"
            )
            rows = cursor.fetchall()
        return [
            {
                "id": str(row[0]),
                "user_name": row[1],
                "boxid": row[2],
                "ttn": row[3],
                "created_at": row[4],
            }
            for row in rows
        ]

    def clear_tracking_records(self, ids: Optional[List[int]] = None) -> None:
        with self._connect() as conn:
            if ids:
                conn.executemany(
                    "DELETE FROM offline_records WHERE id = ?",
                    [(item,) for item in ids],
                )
            else:
                conn.execute("DELETE FROM offline_records")
            conn.commit()

    def add_scanpak_record(self, digits: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO scanpak_offline_scans (parcel_number, stored_at)
                VALUES (?, ?)
                """,
                (digits, datetime.utcnow().isoformat()),
            )
            conn.commit()

    def list_scanpak_records(self) -> List[Dict[str, str]]:
        with self._connect() as conn:
            cursor = conn.execute(
                "SELECT id, parcel_number, stored_at FROM scanpak_offline_scans"
            )
            rows = cursor.fetchall()
        return [
            {
                "id": str(row[0]),
                "parcel_number": row[1],
                "stored_at": row[2],
            }
            for row in rows
        ]

    def remove_scanpak_record(self, record_id: int) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM scanpak_offline_scans WHERE id = ?", (record_id,))
            conn.commit()

    def scanpak_contains(self, digits: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                "SELECT 1 FROM scanpak_offline_scans WHERE parcel_number = ? LIMIT 1",
                (digits,),
            )
            return cursor.fetchone() is not None


@dataclass
class UserRoleInfo:
    label: str
    level: int
    can_clear_history: bool
    can_clear_errors: bool
    is_admin: bool


def parse_user_role(raw: Optional[str], level: Optional[int]) -> UserRoleInfo:
    if raw == "admin" or level == 1:
        return UserRoleInfo(
            label="🔑 Адмін",
            level=1,
            can_clear_history=True,
            can_clear_errors=True,
            is_admin=True,
        )
    if raw == "operator" or level == 0:
        return UserRoleInfo(
            label="🧰 Оператор",
            level=0,
            can_clear_history=False,
            can_clear_errors=True,
            is_admin=False,
        )
    return UserRoleInfo(
        label="👁 Перегляд",
        level=2,
        can_clear_history=False,
        can_clear_errors=False,
        is_admin=False,
    )


def run_in_thread(fn: Callable[[], Any], on_success: Callable[[Any], None], on_error: Callable[[str], None]) -> None:
    def runner() -> None:
        try:
            result = fn()
            on_success(result)
        except Exception as exc:  # pylint: disable=broad-except
            on_error(str(exc))

    threading.Thread(target=runner, daemon=True).start()


def sanitize_digits(value: str) -> str:
    return "".join(ch for ch in value if ch.isdigit())


def format_iso_datetime(value: Any) -> str:
    if not isinstance(value, str):
        return str(value or "")
    try:
        return datetime.fromisoformat(value).astimezone().strftime("%d.%m.%Y %H:%M:%S")
    except ValueError:
        return value


class AppState:
    def __init__(self) -> None:
        self.settings = Settings()
        self.store = OfflineStore()


class TrackingAppUI:
    def __init__(self, page: ft.Page) -> None:
        self.page = page
        self.state = AppState()
        self.page.title = "TrackingApp"
        self.page.theme_mode = ft.ThemeMode.LIGHT
        self.page.theme = ft.Theme(color_scheme_seed=ft.colors.BLUE)
        self.page.window_width = 1200
        self.page.window_height = 800
        self.page.scroll = ft.ScrollMode.AUTO
        self._build()

    def _build(self) -> None:
        self.page.controls.clear()
        self._show_start()

    def _show_start(self) -> None:
        self.page.controls = [
            ft.Container(
                expand=True,
                alignment=ft.alignment.center,
                content=ft.Column(
                    width=420,
                    horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                    controls=[
                        ft.Text("Оберіть режим", size=28, weight=ft.FontWeight.BOLD),
                        ft.ElevatedButton(
                            text="Увійти в TrackingApp",
                            width=300,
                            on_click=lambda _: self._show_tracking_login(),
                        ),
                        ft.ElevatedButton(
                            text="Увійти в СканПак",
                            width=300,
                            on_click=lambda _: self._show_scanpak_login(),
                        ),
                    ],
                ),
            )
        ]
        self.page.update()

    def _show_tracking_login(self) -> None:
        self._show_login(
            title="TrackingApp",
            on_login=self._tracking_login,
            on_register=self._tracking_register,
            on_admin=self._open_admin_panel,
        )

    def _show_scanpak_login(self) -> None:
        self._show_login(
            title="СканПак",
            on_login=self._scanpak_login,
            on_register=self._scanpak_register,
            on_admin=self._open_scanpak_admin_panel,
        )

    def _show_login(
        self,
        title: str,
        on_login: Callable[[str, str, ft.Text], None],
        on_register: Callable[[str, str, ft.Text], None],
        on_admin: Callable[[], None],
    ) -> None:
        surname = ft.TextField(label="Прізвище", width=320)
        password = ft.TextField(label="Пароль", password=True, width=320)
        confirm = ft.TextField(label="Підтвердіть пароль", password=True, width=320, visible=False)
        status = ft.Text("")
        is_register = {"value": False}

        def toggle_mode(_: ft.ControlEvent) -> None:
            is_register["value"] = not is_register["value"]
            confirm.visible = is_register["value"]
            submit.text = "Зареєструватися" if is_register["value"] else "Увійти"
            toggle.text = "Повернутися до входу" if is_register["value"] else "Зареєструватися"
            status.value = ""
            self.page.update()

        def submit_action(_: ft.ControlEvent) -> None:
            if not surname.value or not password.value:
                status.value = "Заповніть прізвище та пароль."
                self.page.update()
                return
            if is_register["value"]:
                if len(password.value) < 6:
                    status.value = "Пароль має містити мінімум 6 символів."
                    self.page.update()
                    return
                if password.value != confirm.value:
                    status.value = "Паролі не співпадають."
                    self.page.update()
                    return
                on_register(surname.value.strip(), password.value.strip(), status)
            else:
                on_login(surname.value.strip(), password.value.strip(), status)

        submit = ft.ElevatedButton(text="Увійти", width=320, on_click=submit_action)
        toggle = ft.TextButton(text="Зареєструватися", on_click=toggle_mode)

        self.page.controls = [
            ft.Container(
                expand=True,
                alignment=ft.alignment.center,
                content=ft.Column(
                    width=420,
                    horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                    controls=[
                        ft.Text(title, size=30, weight=ft.FontWeight.BOLD),
                        surname,
                        password,
                        confirm,
                        status,
                        submit,
                        toggle,
                        ft.TextButton(text="Адмін-панель", on_click=lambda _: on_admin()),
                        ft.TextButton(text="Назад", on_click=lambda _: self._show_start()),
                    ],
                ),
            )
        ]
        self.page.update()

    def _tracking_login(self, surname: str, password: str, status: ft.Text) -> None:
        status.value = "Вхід..."
        self.page.update()

        def task() -> Dict[str, Any]:
            resp = requests.post(
                f"{api_base_url()}/login",
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                data=json.dumps({"surname": surname, "password": password}),
                timeout=10,
            )
            if resp.status_code != 200:
                raise RuntimeError(resp.text)
            return resp.json()

        def success(data: Dict[str, Any]) -> None:
            token = str(data.get("token", ""))
            if not token:
                status.value = "Сервер не повернув токен"
                self.page.update()
                return
            self.state.settings.set("token", token)
            self.state.settings.set("user_name", data.get("surname") or surname)
            self.state.settings.set("access_level", data.get("access_level"))
            self.state.settings.set("user_role", data.get("role"))
            self._show_tracking_dashboard()

        def error(msg: str) -> None:
            status.value = msg
            self.page.update()

        run_in_thread(task, success, error)

    def _tracking_register(self, surname: str, password: str, status: ft.Text) -> None:
        status.value = "Надсилання..."
        self.page.update()

        def task() -> None:
            resp = requests.post(
                f"{api_base_url()}/register",
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                data=json.dumps({"surname": surname, "password": password}),
                timeout=10,
            )
            if resp.status_code != 200:
                raise RuntimeError(resp.text)

        def success(_: Any) -> None:
            status.value = "Заявку на реєстрацію відправлено."
            self.page.update()

        def error(msg: str) -> None:
            status.value = msg
            self.page.update()

        run_in_thread(task, success, error)

    def _scanpak_login(self, surname: str, password: str, status: ft.Text) -> None:
        status.value = "Вхід..."
        self.page.update()

        def task() -> Dict[str, Any]:
            resp = requests.post(
                f"{scanpak_base_url()}/login",
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                data=json.dumps({"surname": surname, "password": password}),
                timeout=10,
            )
            if resp.status_code != 200:
                raise RuntimeError(resp.text)
            return resp.json()

        def success(data: Dict[str, Any]) -> None:
            token = str(data.get("token", ""))
            if not token:
                status.value = "Сервер не повернув токен"
                self.page.update()
                return
            self.state.settings.set("scanpak_token", token)
            self.state.settings.set("scanpak_user_name", data.get("surname") or surname)
            self.state.settings.set("scanpak_user_role", data.get("role"))
            self._show_scanpak_home()

        def error(msg: str) -> None:
            status.value = msg
            self.page.update()

        run_in_thread(task, success, error)

    def _scanpak_register(self, surname: str, password: str, status: ft.Text) -> None:
        status.value = "Надсилання..."
        self.page.update()

        def task() -> None:
            resp = requests.post(
                f"{scanpak_base_url()}/register",
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                data=json.dumps({"surname": surname, "password": password}),
                timeout=10,
            )
            if resp.status_code != 200:
                raise RuntimeError(resp.text)

        def success(_: Any) -> None:
            status.value = "Заявку на реєстрацію відправлено."
            self.page.update()

        def error(msg: str) -> None:
            status.value = msg
            self.page.update()

        run_in_thread(task, success, error)

    def _show_tracking_dashboard(self) -> None:
        role_info = parse_user_role(
            self.state.settings.get("user_role"), self.state.settings.get("access_level")
        )
        header = ft.Row(
            controls=[
                ft.Text(f"Оператор: {self.state.settings.get('user_name', 'operator')}", size=16),
                ft.Text(role_info.label, weight=ft.FontWeight.BOLD),
                ft.Container(expand=True),
                ft.TextButton(text="Вийти", on_click=lambda _: self._logout_tracking()),
            ]
        )
        tabs = [
            ft.Tab(text="Сканер", content=self._tracking_scanner_tab()),
            ft.Tab(text="Історія", content=self._tracking_history_tab(role_info)),
            ft.Tab(text="Помилки", content=self._tracking_errors_tab(role_info)),
        ]
        if role_info.is_admin:
            tabs.append(ft.Tab(text="Статистика", content=self._tracking_stats_tab()))

        self.page.controls = [
            ft.Container(
                padding=20,
                expand=True,
                content=ft.Column(
                    controls=[
                        header,
                        ft.Tabs(tabs=tabs, expand=True),
                    ],
                ),
            )
        ]
        self.page.update()

    def _logout_tracking(self) -> None:
        self.state.settings.clear(["token", "access_level", "user_name", "user_role"])
        self._show_start()

    def _tracking_scanner_tab(self) -> ft.Control:
        boxid = ft.TextField(label="BoxID", width=240)
        ttn = ft.TextField(label="TTN", width=240)
        status = ft.Text("")

        def send(_: ft.ControlEvent) -> None:
            token = self.state.settings.get("token")
            if not token:
                status.value = "Відсутній токен. Увійдіть знову."
                self.page.update()
                return
            box_value = sanitize_digits(boxid.value or "")
            ttn_value = sanitize_digits(ttn.value or "")
            if not box_value or not ttn_value:
                status.value = "Заповніть BoxID і TTN"
                self.page.update()
                return

            record = {
                "user_name": self.state.settings.get("user_name", "operator"),
                "boxid": box_value,
                "ttn": ttn_value,
            }

            def task() -> str:
                resp = requests.post(
                    f"{api_base_url()}/add_record",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    data=json.dumps(record),
                    timeout=10,
                )
                if resp.status_code == 200:
                    return "ok"
                raise RuntimeError(resp.text)

            def success(_: Any) -> None:
                status.value = "✅ Запис надіслано"
                boxid.value = ""
                ttn.value = ""
                self.page.update()

            def error(_: str) -> None:
                status.value = "⚠️ Немає зв'язку, запис збережено офлайн"
                self.state.store.add_tracking_record(record)
                self.page.update()

            status.value = "Надсилання..."
            self.page.update()
            run_in_thread(task, success, error)

        def sync(_: ft.ControlEvent) -> None:
            token = self.state.settings.get("token")
            if not token:
                status.value = "Відсутній токен. Увійдіть знову."
                self.page.update()
                return

            def task() -> str:
                records = self.state.store.list_tracking_records()
                if not records:
                    return "Немає офлайн записів"
                sent_ids: List[int] = []
                for record in records:
                    resp = requests.post(
                        f"{api_base_url()}/add_record",
                        headers={
                            "Authorization": f"Bearer {token}",
                            "Content-Type": "application/json",
                        },
                        data=json.dumps(
                            {
                                "user_name": record["user_name"],
                                "boxid": record["boxid"],
                                "ttn": record["ttn"],
                            }
                        ),
                        timeout=10,
                    )
                    if resp.status_code == 200:
                        sent_ids.append(int(record["id"]))
                if sent_ids:
                    self.state.store.clear_tracking_records(sent_ids)
                return f"Синхронізовано: {len(sent_ids)}"

            def success(message: str) -> None:
                status.value = message
                self.page.update()

            def error(message: str) -> None:
                status.value = f"Помилка: {message}"
                self.page.update()

            status.value = "Синхронізація..."
            self.page.update()
            run_in_thread(task, success, error)

        return ft.Column(
            controls=[
                ft.Row(controls=[boxid, ttn]),
                ft.Row(
                    controls=[
                        ft.ElevatedButton(text="Надіслати", on_click=send),
                        ft.OutlinedButton(text="Синхронізувати офлайн", on_click=sync),
                    ]
                ),
                status,
            ]
        )

    def _tracking_history_tab(self, role_info: UserRoleInfo) -> ft.Control:
        box_filter = ft.TextField(label="BoxID")
        ttn_filter = ft.TextField(label="TTN")
        user_filter = ft.TextField(label="Користувач")
        date_filter = ft.DatePicker(on_change=lambda _: None)
        date_button = ft.ElevatedButton(
            text="Дата", on_click=lambda _: self.page.show_date_picker(date_filter)
        )
        start_time = ft.TimePicker()
        start_button = ft.ElevatedButton(
            text="Початок", on_click=lambda _: self.page.show_time_picker(start_time)
        )
        end_time = ft.TimePicker()
        end_button = ft.ElevatedButton(
            text="Кінець", on_click=lambda _: self.page.show_time_picker(end_time)
        )
        status = ft.Text("")

        table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("BoxID")),
                ft.DataColumn(ft.Text("TTN")),
                ft.DataColumn(ft.Text("Оператор")),
                ft.DataColumn(ft.Text("Дата")),
            ],
            rows=[],
        )
        records: List[Dict[str, Any]] = []

        def apply_filters(_: Any = None) -> None:
            filtered = list(records)
            if box_filter.value:
                filtered = [
                    item for item in filtered if box_filter.value in str(item.get("boxid", ""))
                ]
            if ttn_filter.value:
                filtered = [
                    item for item in filtered if ttn_filter.value in str(item.get("ttn", ""))
                ]
            if user_filter.value:
                filtered = [
                    item
                    for item in filtered
                    if user_filter.value.lower()
                    in str(item.get("user_name", "")).lower()
                ]
            if date_filter.value:
                selected = date_filter.value
                filtered = [
                    item
                    for item in filtered
                    if item.get("datetime")
                    and datetime.fromisoformat(item["datetime"]).date() == selected
                ]

            if start_time.value or end_time.value:
                start_val = start_time.value or ft.Time(0, 0)
                end_val = end_time.value or ft.Time(23, 59)
                start_dt = datetime.combine(date.today(), datetime.strptime(str(start_val), "%H:%M").time())
                end_dt = datetime.combine(date.today(), datetime.strptime(str(end_val), "%H:%M").time())

                def within(item: Dict[str, Any]) -> bool:
                    if not item.get("datetime"):
                        return False
                    try:
                        parsed = datetime.fromisoformat(item["datetime"]).astimezone()
                    except ValueError:
                        return False
                    return start_dt.time() <= parsed.time() <= end_dt.time()

                filtered = [item for item in filtered if within(item)]

            table.rows = [
                ft.DataRow(
                    cells=[
                        ft.DataCell(ft.Text(str(item.get("boxid", "")))),
                        ft.DataCell(ft.Text(str(item.get("ttn", "")))),
                        ft.DataCell(ft.Text(str(item.get("user_name", "")))),
                        ft.DataCell(ft.Text(format_iso_datetime(item.get("datetime")))),
                    ]
                )
                for item in filtered
            ]
            self.page.update()

        def fetch(_: ft.ControlEvent) -> None:
            token = self.state.settings.get("token")
            if not token:
                return

            def task() -> List[Dict[str, Any]]:
                resp = requests.get(
                    f"{api_base_url()}/get_history",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
                data.sort(
                    key=lambda item: datetime.fromisoformat(
                        item.get("datetime") or "1970-01-01T00:00:00"
                    ),
                    reverse=True,
                )
                return data

            def success(data: List[Dict[str, Any]]) -> None:
                records.clear()
                records.extend(data)
                status.value = f"Записів: {len(records)}"
                apply_filters()

            def error(message: str) -> None:
                status.value = f"Помилка: {message}"
                self.page.update()

            run_in_thread(task, success, error)

        def clear_filters(_: ft.ControlEvent) -> None:
            box_filter.value = ""
            ttn_filter.value = ""
            user_filter.value = ""
            date_filter.value = None
            start_time.value = None
            end_time.value = None
            apply_filters()

        def clear_history(_: ft.ControlEvent) -> None:
            if not role_info.can_clear_history:
                return
            token = self.state.settings.get("token")
            if not token:
                return

            def task() -> str:
                resp = requests.delete(
                    f"{api_base_url()}/clear_tracking",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=10,
                )
                if resp.status_code != 200:
                    raise RuntimeError(resp.text)
                return "Історію очищено"

            def success(message: str) -> None:
                records.clear()
                status.value = message
                apply_filters()

            def error(message: str) -> None:
                status.value = f"Помилка: {message}"
                self.page.update()

            run_in_thread(task, success, error)

        buttons = [
            ft.ElevatedButton(text="Оновити", on_click=fetch),
            ft.OutlinedButton(text="Застосувати фільтри", on_click=lambda _: apply_filters()),
            ft.TextButton(text="Скинути", on_click=clear_filters),
        ]
        if role_info.can_clear_history:
            buttons.append(ft.TextButton(text="Очистити історію", on_click=clear_history))

        return ft.Column(
            controls=[
                ft.Row(controls=[box_filter, ttn_filter, user_filter]),
                ft.Row(controls=[date_button, start_button, end_button]),
                ft.Row(controls=buttons),
                status,
                table,
            ]
        )

    def _tracking_errors_tab(self, role_info: UserRoleInfo) -> ft.Control:
        status = ft.Text("")
        table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("ID")),
                ft.DataColumn(ft.Text("BoxID")),
                ft.DataColumn(ft.Text("TTN")),
                ft.DataColumn(ft.Text("Дата")),
            ],
            rows=[],
        )
        errors: List[Dict[str, Any]] = []

        def fetch(_: ft.ControlEvent) -> None:
            token = self.state.settings.get("token")
            if not token:
                return

            def task() -> List[Dict[str, Any]]:
                resp = requests.get(
                    f"{api_base_url()}/get_errors",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
                data.sort(
                    key=lambda item: datetime.fromisoformat(
                        item.get("datetime") or "1970-01-01T00:00:00"
                    ),
                    reverse=True,
                )
                return data

            def success(data: List[Dict[str, Any]]) -> None:
                errors.clear()
                errors.extend(data)
                table.rows = [
                    ft.DataRow(
                        cells=[
                            ft.DataCell(ft.Text(str(item.get("id", "")))),
                            ft.DataCell(ft.Text(str(item.get("boxid", "")))),
                            ft.DataCell(ft.Text(str(item.get("ttn", "")))),
                            ft.DataCell(ft.Text(format_iso_datetime(item.get("datetime")))),
                        ]
                    )
                    for item in errors
                ]
                status.value = f"Помилок: {len(errors)}"
                self.page.update()

            def error(message: str) -> None:
                status.value = f"Помилка: {message}"
                self.page.update()

            run_in_thread(task, success, error)

        def clear_errors(_: ft.ControlEvent) -> None:
            if not role_info.can_clear_errors:
                return
            token = self.state.settings.get("token")
            if not token:
                return

            def task() -> str:
                resp = requests.delete(
                    f"{api_base_url()}/clear_errors",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=10,
                )
                if resp.status_code != 200:
                    raise RuntimeError(resp.text)
                return "Журнал очищено"

            def success(message: str) -> None:
                errors.clear()
                table.rows = []
                status.value = message
                self.page.update()

            def error(message: str) -> None:
                status.value = f"Помилка: {message}"
                self.page.update()

            run_in_thread(task, success, error)

        def delete_selected(_: ft.ControlEvent) -> None:
            if not role_info.can_clear_errors:
                return
            if not table.rows:
                return
            selected = None
            for row in table.rows:
                if row.selected:
                    selected = row
                    break
            if not selected:
                status.value = "Оберіть рядок"
                self.page.update()
                return
            error_id = selected.cells[0].content.value
            token = self.state.settings.get("token")
            if not token:
                return

            def task() -> str:
                resp = requests.delete(
                    f"{api_base_url()}/delete_error/{error_id}",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=10,
                )
                if resp.status_code != 200:
                    raise RuntimeError(resp.text)
                return "Помилку видалено"

            def success(message: str) -> None:
                status.value = message
                fetch(ft.ControlEvent(None))

            def error(message: str) -> None:
                status.value = f"Помилка: {message}"
                self.page.update()

            run_in_thread(task, success, error)

        buttons = [ft.ElevatedButton(text="Оновити", on_click=fetch)]
        if role_info.can_clear_errors:
            buttons.extend(
                [
                    ft.TextButton(text="Очистити журнал", on_click=clear_errors),
                    ft.TextButton(text="Видалити вибраний", on_click=delete_selected),
                ]
            )

        return ft.Column(controls=[ft.Row(controls=buttons), status, table])

    def _tracking_stats_tab(self) -> ft.Control:
        status = ft.Text("")
        summary = ft.Text("", selectable=True)
        table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("Дата")),
                ft.DataColumn(ft.Text("Сканів")),
                ft.DataColumn(ft.Text("Помилок")),
            ],
            rows=[],
        )

        start_date = ft.DatePicker(value=date.today())
        end_date = ft.DatePicker(value=date.today())
        start_button = ft.ElevatedButton(
            text="Початок", on_click=lambda _: self.page.show_date_picker(start_date)
        )
        end_button = ft.ElevatedButton(
            text="Кінець", on_click=lambda _: self.page.show_date_picker(end_date)
        )

        def fetch(_: ft.ControlEvent) -> None:
            token = self.state.settings.get("token")
            if not token:
                return

            def task() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
                headers = {"Authorization": f"Bearer {token}"}
                history_resp = requests.get(
                    f"{api_base_url()}/get_history", headers=headers, timeout=10
                )
                errors_resp = requests.get(
                    f"{api_base_url()}/get_errors", headers=headers, timeout=10
                )
                history_resp.raise_for_status()
                errors_resp.raise_for_status()
                return history_resp.json(), errors_resp.json()

            def success(data: Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]) -> None:
                history, errors = data
                start_val = start_date.value or date.today()
                end_val = end_date.value or date.today()
                if start_val > end_val:
                    start_val, end_val = end_val, start_val

                def within(item: Dict[str, Any]) -> bool:
                    if not item.get("datetime"):
                        return False
                    try:
                        dt = datetime.fromisoformat(item["datetime"]).astimezone().date()
                    except ValueError:
                        return False
                    return start_val <= dt <= end_val

                history_filtered = [item for item in history if within(item)]
                errors_filtered = [item for item in errors if within(item)]

                scan_counts: Dict[str, int] = {}
                error_counts: Dict[str, int] = {}
                daily_scans: Dict[date, int] = {}
                daily_errors: Dict[date, int] = {}

                for item in history_filtered:
                    user = str(item.get("user_name", "—"))
                    scan_counts[user] = scan_counts.get(user, 0) + 1
                    try:
                        dt = datetime.fromisoformat(item["datetime"]).astimezone().date()
                        daily_scans[dt] = daily_scans.get(dt, 0) + 1
                    except ValueError:
                        continue

                for item in errors_filtered:
                    user = str(item.get("user_name", "—"))
                    error_counts[user] = error_counts.get(user, 0) + 1
                    try:
                        dt = datetime.fromisoformat(item["datetime"]).astimezone().date()
                        daily_errors[dt] = daily_errors.get(dt, 0) + 1
                    except ValueError:
                        continue

                top_user, top_count = self._top_item(scan_counts)
                top_error_user, top_error_count = self._top_item(error_counts)

                summary.value = (
                    f"Усього сканів: {sum(scan_counts.values())}\n"
                    f"Унікальних операторів: {len(scan_counts)}\n"
                    f"Усього помилок: {sum(error_counts.values())}\n"
                    f"Найактивніший оператор: {top_user} ({top_count})\n"
                    f"Оператор з найбільшою кількістю помилок: {top_error_user} ({top_error_count})"
                )

                all_dates = sorted(set(daily_scans.keys()) | set(daily_errors.keys()))
                table.rows = [
                    ft.DataRow(
                        cells=[
                            ft.DataCell(ft.Text(day.strftime("%d.%m.%Y"))),
                            ft.DataCell(ft.Text(str(daily_scans.get(day, 0)))),
                            ft.DataCell(ft.Text(str(daily_errors.get(day, 0)))),
                        ]
                    )
                    for day in all_dates
                ]
                status.value = "Оновлено"
                self.page.update()

            def error(message: str) -> None:
                status.value = f"Помилка: {message}"
                self.page.update()

            run_in_thread(task, success, error)

        return ft.Column(
            controls=[
                ft.Row(controls=[start_button, end_button, ft.ElevatedButton(text="Оновити", on_click=fetch)]),
                status,
                summary,
                table,
            ]
        )

    def _top_item(self, data: Dict[str, int]) -> Tuple[str, int]:
        if not data:
            return "—", 0
        top_user = max(data.items(), key=lambda item: item[1])
        return top_user[0], top_user[1]

    def _show_scanpak_home(self) -> None:
        header = ft.Row(
            controls=[
                ft.Text(
                    f"Оператор: {self.state.settings.get('scanpak_user_name', '—')}",
                    size=16,
                ),
                ft.Container(expand=True),
                ft.TextButton(text="Вийти", on_click=lambda _: self._logout_scanpak()),
            ]
        )

        tabs = ft.Tabs(
            tabs=[
                ft.Tab(text="Сканування", content=self._scanpak_scan_tab()),
                ft.Tab(text="Історія", content=self._scanpak_history_tab()),
                ft.Tab(text="Статистика", content=self._scanpak_stats_tab()),
            ],
            expand=True,
        )

        self.page.controls = [
            ft.Container(
                padding=20,
                expand=True,
                content=ft.Column(controls=[header, tabs]),
            )
        ]
        self.page.update()

    def _logout_scanpak(self) -> None:
        self.state.settings.clear(["scanpak_token", "scanpak_user_name", "scanpak_user_role"])
        self._show_start()

    def _scanpak_scan_tab(self) -> ft.Control:
        number = ft.TextField(label="Номер відправлення", width=320)
        status = ft.Text("")

        def send(_: ft.ControlEvent) -> None:
            token = self.state.settings.get("scanpak_token")
            if not token:
                status.value = "Відсутній токен. Увійдіть знову."
                self.page.update()
                return
            digits = sanitize_digits(number.value or "")
            if not digits:
                status.value = "Введіть номер відправлення"
                self.page.update()
                return

            def task() -> str:
                resp = requests.post(
                    f"{scanpak_base_url()}/scans",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                    data=json.dumps({"parcel_number": digits}),
                    timeout=10,
                )
                if resp.status_code == 200:
                    return "ok"
                raise RuntimeError(resp.text)

            def success(_: Any) -> None:
                status.value = "✅ Скан збережено"
                number.value = ""
                self.page.update()

            def error(_: str) -> None:
                status.value = "⚠️ Немає зв'язку, запис збережено офлайн"
                if not self.state.store.scanpak_contains(digits):
                    self.state.store.add_scanpak_record(digits)
                self.page.update()

            status.value = "Надсилання..."
            self.page.update()
            run_in_thread(task, success, error)

        def sync(_: ft.ControlEvent) -> None:
            token = self.state.settings.get("scanpak_token")
            if not token:
                status.value = "Відсутній токен. Увійдіть знову."
                self.page.update()
                return

            def task() -> str:
                records = self.state.store.list_scanpak_records()
                if not records:
                    return "Немає офлайн записів"
                sent = 0
                for record in records:
                    resp = requests.post(
                        f"{scanpak_base_url()}/scans",
                        headers={
                            "Authorization": f"Bearer {token}",
                            "Content-Type": "application/json",
                            "Accept": "application/json",
                        },
                        data=json.dumps({"parcel_number": record["parcel_number"]}),
                        timeout=10,
                    )
                    if resp.status_code == 200:
                        sent += 1
                        self.state.store.remove_scanpak_record(int(record["id"]))
                return f"Синхронізовано: {sent}"

            def success(message: str) -> None:
                status.value = message
                self.page.update()

            def error(message: str) -> None:
                status.value = f"Помилка: {message}"
                self.page.update()

            status.value = "Синхронізація..."
            self.page.update()
            run_in_thread(task, success, error)

        return ft.Column(
            controls=[
                number,
                ft.Row(
                    controls=[
                        ft.ElevatedButton(text="Надіслати", on_click=send),
                        ft.OutlinedButton(text="Синхронізувати офлайн", on_click=sync),
                    ]
                ),
                status,
            ]
        )

    def _scanpak_history_tab(self) -> ft.Control:
        parcel_filter = ft.TextField(label="Номер")
        user_filter = ft.TextField(label="Користувач")
        date_filter = ft.DatePicker()
        date_button = ft.ElevatedButton(
            text="Дата", on_click=lambda _: self.page.show_date_picker(date_filter)
        )
        status = ft.Text("")
        table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("Номер")),
                ft.DataColumn(ft.Text("Користувач")),
                ft.DataColumn(ft.Text("Дата")),
            ],
            rows=[],
        )
        records: List[Dict[str, Any]] = []

        def apply_filters(_: Any = None) -> None:
            filtered = list(records)
            if parcel_filter.value:
                filtered = [
                    item
                    for item in filtered
                    if parcel_filter.value in str(item.get("parcel_number", ""))
                ]
            if user_filter.value:
                filtered = [
                    item
                    for item in filtered
                    if user_filter.value.lower()
                    in str(item.get("user_name", "")).lower()
                ]
            if date_filter.value:
                target = date_filter.value
                filtered = [
                    item
                    for item in filtered
                    if item.get("created_at")
                    and datetime.fromisoformat(item["created_at"]).date() == target
                ]

            table.rows = [
                ft.DataRow(
                    cells=[
                        ft.DataCell(ft.Text(str(item.get("parcel_number", "")))),
                        ft.DataCell(ft.Text(str(item.get("user_name", "")))),
                        ft.DataCell(ft.Text(format_iso_datetime(item.get("created_at")))),
                    ]
                )
                for item in filtered
            ]
            self.page.update()

        def fetch(_: ft.ControlEvent) -> None:
            token = self.state.settings.get("scanpak_token")
            if not token:
                return

            def task() -> List[Dict[str, Any]]:
                resp = requests.get(
                    f"{scanpak_base_url()}/history",
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
                data.sort(
                    key=lambda item: datetime.fromisoformat(
                        item.get("created_at") or "1970-01-01T00:00:00"
                    ),
                    reverse=True,
                )
                return data

            def success(data: List[Dict[str, Any]]) -> None:
                records.clear()
                records.extend(data)
                status.value = f"Записів: {len(records)}"
                apply_filters()

            def error(message: str) -> None:
                status.value = f"Помилка: {message}"
                self.page.update()

            run_in_thread(task, success, error)

        return ft.Column(
            controls=[
                ft.Row(controls=[parcel_filter, user_filter, date_button]),
                ft.Row(
                    controls=[
                        ft.ElevatedButton(text="Оновити", on_click=fetch),
                        ft.OutlinedButton(text="Застосувати", on_click=lambda _: apply_filters()),
                    ]
                ),
                status,
                table,
            ]
        )

    def _scanpak_stats_tab(self) -> ft.Control:
        start_date = ft.DatePicker(value=date.today())
        end_date = ft.DatePicker(value=date.today())
        start_button = ft.ElevatedButton(
            text="Початок", on_click=lambda _: self.page.show_date_picker(start_date)
        )
        end_button = ft.ElevatedButton(
            text="Кінець", on_click=lambda _: self.page.show_date_picker(end_date)
        )
        user_filter = ft.TextField(label="Користувач")
        summary = ft.Text("")
        status = ft.Text("")
        records: List[Dict[str, Any]] = []

        def fetch(_: ft.ControlEvent) -> None:
            token = self.state.settings.get("scanpak_token")
            if not token:
                return

            def task() -> List[Dict[str, Any]]:
                resp = requests.get(
                    f"{scanpak_base_url()}/history",
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    timeout=10,
                )
                resp.raise_for_status()
                return resp.json()

            def success(data: List[Dict[str, Any]]) -> None:
                records.clear()
                records.extend(data)
                apply_filters()

            def error(message: str) -> None:
                status.value = f"Помилка: {message}"
                self.page.update()

            run_in_thread(task, success, error)

        def apply_filters(_: Any = None) -> None:
            if not records:
                return
            start_val = start_date.value or date.today()
            end_val = end_date.value or date.today()
            if start_val > end_val:
                start_val, end_val = end_val, start_val
            user_value = user_filter.value.lower() if user_filter.value else ""

            filtered: List[Dict[str, Any]] = []
            for item in records:
                if not item.get("created_at"):
                    continue
                try:
                    dt = datetime.fromisoformat(item["created_at"]).astimezone().date()
                except ValueError:
                    continue
                if not (start_val <= dt <= end_val):
                    continue
                if user_value and user_value not in str(item.get("user_name", "")).lower():
                    continue
                filtered.append(item)

            per_user: Dict[str, int] = {}
            for item in filtered:
                user = str(item.get("user_name", "—"))
                per_user[user] = per_user.get(user, 0) + 1

            top_user, top_count = self._top_item(per_user)
            summary.value = (
                f"Сканів у періоді: {len(filtered)}\n"
                f"Унікальних користувачів: {len(per_user)}\n"
                f"Найактивніший оператор: {top_user} ({top_count})"
            )
            status.value = "Оновлено"
            self.page.update()

        return ft.Column(
            controls=[
                ft.Row(controls=[start_button, end_button, user_filter]),
                ft.Row(
                    controls=[
                        ft.ElevatedButton(text="Оновити", on_click=fetch),
                        ft.OutlinedButton(text="Застосувати", on_click=lambda _: apply_filters()),
                    ]
                ),
                status,
                summary,
            ]
        )

    def _open_admin_panel(self) -> None:
        def submit(password: str) -> None:
            if not password:
                return

            def task() -> Dict[str, Any]:
                resp = requests.post(
                    f"{api_base_url()}/admin_login",
                    headers={"Accept": "application/json", "Content-Type": "application/json"},
                    data=json.dumps({"password": password}),
                    timeout=10,
                )
                if resp.status_code != 200:
                    raise RuntimeError(resp.text)
                return resp.json()

            def success(data: Dict[str, Any]) -> None:
                token = data.get("token")
                if not token:
                    self._show_dialog("Помилка", "Сервер не повернув токен")
                    return
                self._show_admin_dialog(token, is_scanpak=False)

            def error(message: str) -> None:
                self._show_dialog("Помилка", message)

            run_in_thread(task, success, error)

        self._ask_password("Адмін-панель", submit)

    def _open_scanpak_admin_panel(self) -> None:
        def submit(password: str) -> None:
            if not password:
                return

            def task() -> Dict[str, Any]:
                resp = requests.post(
                    f"{scanpak_base_url()}/admin_login",
                    headers={"Accept": "application/json", "Content-Type": "application/json"},
                    data=json.dumps({"password": password}),
                    timeout=10,
                )
                if resp.status_code != 200:
                    raise RuntimeError(resp.text)
                return resp.json()

            def success(data: Dict[str, Any]) -> None:
                token = data.get("token")
                if not token:
                    self._show_dialog("Помилка", "Сервер не повернув токен")
                    return
                self._show_admin_dialog(token, is_scanpak=True)

            def error(message: str) -> None:
                self._show_dialog("Помилка", message)

            run_in_thread(task, success, error)

        self._ask_password("Адмін-панель СканПак", submit)

    def _ask_password(self, title: str, on_submit: Callable[[str], None]) -> None:
        password_field = ft.TextField(label="Пароль", password=True)

        def submit(_: ft.ControlEvent) -> None:
            self.page.dialog.open = False
            self.page.update()
            on_submit(password_field.value.strip())

        dialog = ft.AlertDialog(
            title=ft.Text(title),
            content=password_field,
            actions=[ft.TextButton(text="Скасувати", on_click=lambda _: self._close_dialog()),
                     ft.ElevatedButton(text="Увійти", on_click=submit)],
        )
        self.page.dialog = dialog
        dialog.open = True
        self.page.update()

    def _show_admin_dialog(self, token: str, is_scanpak: bool) -> None:
        role_options = ["admin", "operator"] if is_scanpak else ["admin", "operator", "viewer"]
        pending_table = ft.DataTable(columns=[
            ft.DataColumn(ft.Text("ID")),
            ft.DataColumn(ft.Text("Прізвище")),
            ft.DataColumn(ft.Text("Дата")),
        ], rows=[])
        users_table = ft.DataTable(columns=[
            ft.DataColumn(ft.Text("ID")),
            ft.DataColumn(ft.Text("Прізвище")),
            ft.DataColumn(ft.Text("Роль")),
            ft.DataColumn(ft.Text("Активний")),
            ft.DataColumn(ft.Text("Створено")),
        ], rows=[])
        role_picker = ft.Dropdown(options=[ft.dropdown.Option(r) for r in role_options])
        role_picker.value = role_options[0]
        password_fields = {role: ft.TextField(label=f"{role} пароль", password=True) for role in role_options}

        def headers() -> Dict[str, str]:
            return {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }

        def load_data() -> None:
            base = scanpak_base_url() if is_scanpak else api_base_url()

            def task() -> Dict[str, Any]:
                pending_resp = requests.get(f"{base}/admin/registration_requests", headers=headers(), timeout=10)
                pending_resp.raise_for_status()
                users_resp = requests.get(f"{base}/admin/users", headers=headers(), timeout=10)
                users_resp.raise_for_status()
                pass_resp = requests.get(f"{base}/admin/role-passwords", headers=headers(), timeout=10)
                pass_resp.raise_for_status()
                return {
                    "pending": pending_resp.json(),
                    "users": users_resp.json(),
                    "passwords": pass_resp.json(),
                }

            def success(data: Dict[str, Any]) -> None:
                pending_table.rows = [
                    ft.DataRow(
                        cells=[
                            ft.DataCell(ft.Text(str(item.get("id", "")))),
                            ft.DataCell(ft.Text(item.get("surname", "—"))),
                            ft.DataCell(ft.Text(item.get("created_at", ""))),
                        ]
                    )
                    for item in data.get("pending", [])
                ]
                users_table.rows = [
                    ft.DataRow(
                        cells=[
                            ft.DataCell(ft.Text(str(item.get("id", "")))),
                            ft.DataCell(ft.Text(item.get("surname", "—"))),
                            ft.DataCell(ft.Text(item.get("role", ""))),
                            ft.DataCell(ft.Text("Так" if item.get("is_active") else "Ні")),
                            ft.DataCell(ft.Text(item.get("created_at", ""))),
                        ]
                    )
                    for item in data.get("users", [])
                ]
                for role, field in password_fields.items():
                    field.value = str(data.get("passwords", {}).get(role, ""))
                self.page.update()

            def error(message: str) -> None:
                self._show_dialog("Помилка", message)

            run_in_thread(task, success, error)

        def selected_id(table: ft.DataTable) -> Optional[str]:
            for row in table.rows:
                if row.selected:
                    return row.cells[0].content.value
            return None

        def approve(_: ft.ControlEvent) -> None:
            request_id = selected_id(pending_table)
            if not request_id:
                return
            payload = json.dumps({"role": role_picker.value})
            base = scanpak_base_url() if is_scanpak else api_base_url()

            def task() -> None:
                resp = requests.post(
                    f"{base}/admin/registration_requests/{request_id}/approve",
                    headers=headers(),
                    data=payload,
                    timeout=10,
                )
                resp.raise_for_status()

            run_in_thread(task, lambda _: load_data(), lambda msg: self._show_dialog("Помилка", msg))

        def reject(_: ft.ControlEvent) -> None:
            request_id = selected_id(pending_table)
            if not request_id:
                return
            base = scanpak_base_url() if is_scanpak else api_base_url()

            def task() -> None:
                resp = requests.post(
                    f"{base}/admin/registration_requests/{request_id}/reject",
                    headers=headers(),
                    timeout=10,
                )
                resp.raise_for_status()

            run_in_thread(task, lambda _: load_data(), lambda msg: self._show_dialog("Помилка", msg))

        def change_role(_: ft.ControlEvent) -> None:
            user_id = selected_id(users_table)
            if not user_id:
                return
            base = scanpak_base_url() if is_scanpak else api_base_url()

            def task() -> None:
                resp = requests.patch(
                    f"{base}/admin/users/{user_id}",
                    headers=headers(),
                    data=json.dumps({"role": role_picker.value}),
                    timeout=10,
                )
                resp.raise_for_status()

            run_in_thread(task, lambda _: load_data(), lambda msg: self._show_dialog("Помилка", msg))

        def toggle_active(_: ft.ControlEvent) -> None:
            user_id = selected_id(users_table)
            if not user_id:
                return
            base = scanpak_base_url() if is_scanpak else api_base_url()
            current_row = next((row for row in users_table.rows if row.selected), None)
            is_active = False
            if current_row and len(current_row.cells) > 3:
                is_active = current_row.cells[3].content.value == "Так"

            def task() -> None:
                resp = requests.patch(
                    f"{base}/admin/users/{user_id}",
                    headers=headers(),
                    data=json.dumps({"is_active": not is_active}),
                    timeout=10,
                )
                resp.raise_for_status()

            run_in_thread(task, lambda _: load_data(), lambda msg: self._show_dialog("Помилка", msg))

        def delete_user(_: ft.ControlEvent) -> None:
            user_id = selected_id(users_table)
            if not user_id:
                return
            base = scanpak_base_url() if is_scanpak else api_base_url()

            def task() -> None:
                resp = requests.delete(
                    f"{base}/admin/users/{user_id}",
                    headers=headers(),
                    timeout=10,
                )
                resp.raise_for_status()

            run_in_thread(task, lambda _: load_data(), lambda msg: self._show_dialog("Помилка", msg))

        def save_password(role: str) -> Callable[[ft.ControlEvent], None]:
            def handler(_: ft.ControlEvent) -> None:
                base = scanpak_base_url() if is_scanpak else api_base_url()

                def task() -> None:
                    resp = requests.post(
                        f"{base}/admin/role-passwords/{role}",
                        headers=headers(),
                        data=json.dumps({"password": password_fields[role].value or ""}),
                        timeout=10,
                    )
                    resp.raise_for_status()

                run_in_thread(task, lambda _: self._show_dialog("Готово", "Пароль оновлено"),
                              lambda msg: self._show_dialog("Помилка", msg))

            return handler

        content = ft.Column(
            width=900,
            height=600,
            scroll=ft.ScrollMode.AUTO,
            controls=[
                ft.Text("Запити на реєстрацію", weight=ft.FontWeight.BOLD),
                pending_table,
                ft.Row(
                    controls=[
                        role_picker,
                        ft.ElevatedButton(text="Підтвердити", on_click=approve),
                        ft.OutlinedButton(text="Відхилити", on_click=reject),
                    ]
                ),
                ft.Divider(),
                ft.Text("Користувачі", weight=ft.FontWeight.BOLD),
                users_table,
                ft.Row(
                    controls=[
                        ft.ElevatedButton(text="Змінити роль", on_click=change_role),
                        ft.OutlinedButton(text="Перемкнути активність", on_click=toggle_active),
                        ft.TextButton(text="Видалити", on_click=delete_user),
                    ]
                ),
                ft.Divider(),
                ft.Text("API паролі", weight=ft.FontWeight.BOLD),
                *[
                    ft.Row(
                        controls=[password_fields[role], ft.ElevatedButton(text="Зберегти", on_click=save_password(role))]
                    )
                    for role in role_options
                ],
                ft.ElevatedButton(text="Оновити", on_click=lambda _: load_data()),
            ],
        )

        dialog = ft.AlertDialog(title=ft.Text("Адмін-панель"), content=content)
        self.page.dialog = dialog
        dialog.open = True
        self.page.update()
        load_data()

    def _show_dialog(self, title: str, message: str) -> None:
        dialog = ft.AlertDialog(title=ft.Text(title), content=ft.Text(message))
        self.page.dialog = dialog
        dialog.open = True
        self.page.update()

    def _close_dialog(self) -> None:
        if self.page.dialog:
            self.page.dialog.open = False
            self.page.update()


def main(page: ft.Page) -> None:
    TrackingAppUI(page)


if __name__ == "__main__":
    ft.app(target=main)
