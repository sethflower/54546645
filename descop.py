import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from platformdirs import user_data_dir
from PySide6.QtCore import QDate, QObject, QRunnable, QThreadPool, QTime, Qt, Signal, Slot
from PySide6.QtWidgets import (QAbstractItemView, QApplication, QComboBox,
                               QDateEdit, QDialog, QFormLayout, QGridLayout,
                               QHBoxLayout, QLabel, QInputDialog, QLineEdit,
                               QMainWindow, QMessageBox, QPushButton,
                               QStackedWidget, QTableWidget, QTableWidgetItem,
                               QTabWidget, QTextEdit, QTimeEdit, QVBoxLayout,
                               QWidget)

APP_NAME = "TrackingApp"
APP_VENDOR = "TrackingApp"

TRACKING_API_HOST = "173.242.53.38"
TRACKING_API_PORT = 10000

SCANPAK_API_HOST = "tracking-api-b4jb.onrender.com"
SCANPAK_BASE_PATH = "/scanpak"


def api_base_url() -> str:
    return f"http://{TRACKING_API_HOST}:{TRACKING_API_PORT}"


def scanpak_base_url() -> str:
    return f"https://{SCANPAK_API_HOST}{SCANPAK_BASE_PATH}"


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


class WorkerSignals(QObject):
    success = Signal(object)
    error = Signal(str)


class Worker(QRunnable):
    def __init__(self, fn: Callable[[], Any]) -> None:
        super().__init__()
        self.fn = fn
        self.signals = WorkerSignals()

    @Slot()
    def run(self) -> None:
        try:
            result = self.fn()
            self.signals.success.emit(result)
        except Exception as exc:  # pylint: disable=broad-except
            self.signals.error.emit(str(exc))


class StartPage(QWidget):
    def __init__(self, navigate: Callable[[str], None]) -> None:
        super().__init__()
        self.navigate = navigate
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignCenter)

        title = QLabel("Оберіть режим")
        title.setObjectName("title")
        title.setAlignment(Qt.AlignCenter)

        tracking_btn = QPushButton("Увійти в TrackingApp")
        tracking_btn.clicked.connect(lambda: self.navigate("tracking_login"))

        scanpak_btn = QPushButton("Увійти в СканПак")
        scanpak_btn.clicked.connect(lambda: self.navigate("scanpak_login"))

        layout.addWidget(title)
        layout.addSpacing(16)
        layout.addWidget(tracking_btn)
        layout.addWidget(scanpak_btn)


class LoginPage(QWidget):
    def __init__(
        self,
        title: str,
        on_login: Callable[[str, str], None],
        on_register: Callable[[str, str], None],
        on_admin: Optional[Callable[[], None]] = None,
    ) -> None:
        super().__init__()
        self.on_login = on_login
        self.on_register = on_register
        self.on_admin = on_admin
        self._is_register = False
        self._status_label = QLabel("")
        self._build_ui(title)

    def _build_ui(self, title: str) -> None:
        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignCenter)

        heading = QLabel(title)
        heading.setObjectName("title")
        heading.setAlignment(Qt.AlignCenter)

        form = QFormLayout()
        self.surname_input = QLineEdit()
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.Password)
        self.confirm_input = QLineEdit()
        self.confirm_input.setEchoMode(QLineEdit.Password)

        form.addRow("Прізвище:", self.surname_input)
        form.addRow("Пароль:", self.password_input)
        form.addRow("Підтвердіть пароль:", self.confirm_input)

        self.confirm_input.setVisible(False)

        button_layout = QHBoxLayout()
        self.submit_btn = QPushButton("Увійти")
        self.submit_btn.clicked.connect(self._submit)
        self.toggle_btn = QPushButton("Зареєструватися")
        self.toggle_btn.clicked.connect(self._toggle_mode)
        button_layout.addWidget(self.submit_btn)
        button_layout.addWidget(self.toggle_btn)

        layout.addWidget(heading)
        layout.addSpacing(12)
        layout.addLayout(form)
        layout.addWidget(self._status_label)
        layout.addLayout(button_layout)

        if self.on_admin:
            admin_btn = QPushButton("Адмін-панель")
            admin_btn.clicked.connect(self.on_admin)
            layout.addWidget(admin_btn)

    def _toggle_mode(self) -> None:
        self._is_register = not self._is_register
        self.confirm_input.setVisible(self._is_register)
        self.submit_btn.setText("Зареєструватися" if self._is_register else "Увійти")
        self.toggle_btn.setText("Повернутися до входу" if self._is_register else "Зареєструватися")
        self._status_label.setText("")

    def _submit(self) -> None:
        surname = self.surname_input.text().strip()
        password = self.password_input.text().strip()
        if not surname or not password:
            self._status_label.setText("Заповніть прізвище та пароль.")
            return
        if self._is_register:
            confirm = self.confirm_input.text().strip()
            if len(password) < 6:
                self._status_label.setText("Пароль має містити мінімум 6 символів.")
                return
            if confirm != password:
                self._status_label.setText("Паролі не співпадають.")
                return
            self.on_register(surname, password)
        else:
            self.on_login(surname, password)

    def set_status(self, message: str, success: bool = False) -> None:
        self._status_label.setText(message)
        self._status_label.setProperty("success", success)
        self._status_label.style().unpolish(self._status_label)
        self._status_label.style().polish(self._status_label)

    def reset(self) -> None:
        self.surname_input.clear()
        self.password_input.clear()
        self.confirm_input.clear()
        self._status_label.setText("")
        if self._is_register:
            self._toggle_mode()


class AdminPanelDialog(QDialog):
    def __init__(self, token: str, is_scanpak: bool = False, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.token = token
        self.is_scanpak = is_scanpak
        self.setWindowTitle("Адмін-панель")
        self.resize(900, 600)
        self._thread_pool = QThreadPool.globalInstance()
        self._build_ui()
        self._load_data()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        self.pending_table = QTableWidget(0, 3)
        self.pending_table.setHorizontalHeaderLabels(["ID", "Прізвище", "Дата"])
        self.pending_table.setEditTriggers(QAbstractItemView.NoEditTriggers)

        pending_widget = QWidget()
        pending_layout = QVBoxLayout(pending_widget)
        pending_layout.addWidget(self.pending_table)

        self.pending_actions = QHBoxLayout()
        self.pending_role = QComboBox()
        self.pending_role.addItems(["admin", "operator"]) if self.is_scanpak else self.pending_role.addItems(["admin", "operator", "viewer"])
        approve_btn = QPushButton("Підтвердити")
        reject_btn = QPushButton("Відхилити")
        approve_btn.clicked.connect(self._approve_pending)
        reject_btn.clicked.connect(self._reject_pending)
        self.pending_actions.addWidget(QLabel("Роль:"))
        self.pending_actions.addWidget(self.pending_role)
        self.pending_actions.addWidget(approve_btn)
        self.pending_actions.addWidget(reject_btn)
        pending_layout.addLayout(self.pending_actions)

        self.users_table = QTableWidget(0, 5)
        self.users_table.setHorizontalHeaderLabels(
            ["ID", "Прізвище", "Роль", "Активний", "Створено"]
        )
        self.users_table.setEditTriggers(QAbstractItemView.NoEditTriggers)

        users_widget = QWidget()
        users_layout = QVBoxLayout(users_widget)
        users_layout.addWidget(self.users_table)

        users_action_layout = QHBoxLayout()
        self.user_role_box = QComboBox()
        self.user_role_box.addItems(["admin", "operator"]) if self.is_scanpak else self.user_role_box.addItems(["admin", "operator", "viewer"])
        toggle_btn = QPushButton("Змінити роль")
        toggle_btn.clicked.connect(self._change_role)
        active_btn = QPushButton("Перемкнути активність")
        active_btn.clicked.connect(self._toggle_active)
        delete_btn = QPushButton("Видалити користувача")
        delete_btn.clicked.connect(self._delete_user)
        users_action_layout.addWidget(self.user_role_box)
        users_action_layout.addWidget(toggle_btn)
        users_action_layout.addWidget(active_btn)
        users_action_layout.addWidget(delete_btn)
        users_layout.addLayout(users_action_layout)

        password_widget = QWidget()
        password_layout = QFormLayout(password_widget)
        self.password_fields: Dict[str, QLineEdit] = {}
        roles = ["admin", "operator"] if self.is_scanpak else ["admin", "operator", "viewer"]
        for role in roles:
            field = QLineEdit()
            field.setEchoMode(QLineEdit.Password)
            self.password_fields[role] = field
            save_btn = QPushButton("Зберегти")
            save_btn.clicked.connect(lambda checked=False, r=role: self._save_role_password(r))
            row_widget = QWidget()
            row_layout = QHBoxLayout(row_widget)
            row_layout.addWidget(field)
            row_layout.addWidget(save_btn)
            password_layout.addRow(role.capitalize(), row_widget)

        self.tabs.addTab(pending_widget, "Запити")
        self.tabs.addTab(users_widget, "Користувачі")
        self.tabs.addTab(password_widget, "API паролі")

        refresh_btn = QPushButton("Оновити")
        refresh_btn.clicked.connect(self._load_data)
        layout.addWidget(refresh_btn)

    def _api_url(self, path: str) -> str:
        if self.is_scanpak:
            return f"{scanpak_base_url()}{path}"
        return f"{api_base_url()}{path}"

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _load_data(self) -> None:
        def task() -> Dict[str, Any]:
            pending_resp = requests.get(self._api_url("/admin/registration_requests"), headers=self._headers(), timeout=10)
            pending_resp.raise_for_status()
            users_resp = requests.get(self._api_url("/admin/users"), headers=self._headers(), timeout=10)
            users_resp.raise_for_status()
            password_resp = requests.get(self._api_url("/admin/role-passwords"), headers=self._headers(), timeout=10)
            password_resp.raise_for_status()
            return {
                "pending": pending_resp.json(),
                "users": users_resp.json(),
                "passwords": password_resp.json(),
            }

        worker = Worker(task)
        worker.signals.success.connect(self._populate_data)
        worker.signals.error.connect(self._show_error)
        self._thread_pool.start(worker)

    def _populate_data(self, payload: Dict[str, Any]) -> None:
        pending = payload.get("pending", []) or []
        users = payload.get("users", []) or []
        passwords = payload.get("passwords", {}) or {}

        self.pending_table.setRowCount(len(pending))
        for row, item in enumerate(pending):
            self.pending_table.setItem(row, 0, QTableWidgetItem(str(item.get("id"))))
            self.pending_table.setItem(row, 1, QTableWidgetItem(item.get("surname", "—")))
            self.pending_table.setItem(row, 2, QTableWidgetItem(item.get("created_at", "")))

        self.users_table.setRowCount(len(users))
        for row, item in enumerate(users):
            self.users_table.setItem(row, 0, QTableWidgetItem(str(item.get("id"))))
            self.users_table.setItem(row, 1, QTableWidgetItem(item.get("surname", "—")))
            self.users_table.setItem(row, 2, QTableWidgetItem(item.get("role", "")))
            self.users_table.setItem(row, 3, QTableWidgetItem("Так" if item.get("is_active") else "Ні"))
            self.users_table.setItem(row, 4, QTableWidgetItem(item.get("created_at", "")))

        for role, field in self.password_fields.items():
            field.setText(passwords.get(role, ""))

    def _selected_pending_id(self) -> Optional[int]:
        items = self.pending_table.selectedItems()
        if not items:
            return None
        return int(items[0].text())

    def _selected_user_id(self) -> Optional[int]:
        items = self.users_table.selectedItems()
        if not items:
            return None
        return int(items[0].text())

    def _approve_pending(self) -> None:
        request_id = self._selected_pending_id()
        if request_id is None:
            self._show_error("Оберіть запит для підтвердження.")
            return
        role = self.pending_role.currentText()
        payload = json.dumps({"role": role})

        def task() -> None:
            resp = requests.post(
                self._api_url(f"/admin/registration_requests/{request_id}/approve"),
                headers=self._headers(),
                data=payload,
                timeout=10,
            )
            resp.raise_for_status()

        self._run_action(task, "Користувача підтверджено")

    def _reject_pending(self) -> None:
        request_id = self._selected_pending_id()
        if request_id is None:
            self._show_error("Оберіть запит для відхилення.")
            return

        def task() -> None:
            resp = requests.post(
                self._api_url(f"/admin/registration_requests/{request_id}/reject"),
                headers=self._headers(),
                timeout=10,
            )
            resp.raise_for_status()

        self._run_action(task, "Запит відхилено")

    def _change_role(self) -> None:
        user_id = self._selected_user_id()
        if user_id is None:
            self._show_error("Оберіть користувача для зміни ролі.")
            return
        role = self.user_role_box.currentText()
        payload = json.dumps({"role": role})

        def task() -> None:
            resp = requests.patch(
                self._api_url(f"/admin/users/{user_id}"),
                headers=self._headers(),
                data=payload,
                timeout=10,
            )
            resp.raise_for_status()

        self._run_action(task, "Роль оновлено")

    def _toggle_active(self) -> None:
        user_id = self._selected_user_id()
        if user_id is None:
            self._show_error("Оберіть користувача для зміни активності.")
            return
        current_status_item = self.users_table.item(self.users_table.currentRow(), 3)
        is_active = current_status_item.text().strip() == "Так" if current_status_item else False
        payload = json.dumps({"is_active": not is_active})

        def task() -> None:
            resp = requests.patch(
                self._api_url(f"/admin/users/{user_id}"),
                headers=self._headers(),
                data=payload,
                timeout=10,
            )
            resp.raise_for_status()

        self._run_action(task, "Статус оновлено")

    def _delete_user(self) -> None:
        user_id = self._selected_user_id()
        if user_id is None:
            self._show_error("Оберіть користувача для видалення.")
            return

        if QMessageBox.question(self, "Підтвердження", "Видалити користувача?") != QMessageBox.Yes:
            return

        def task() -> None:
            resp = requests.delete(
                self._api_url(f"/admin/users/{user_id}"),
                headers=self._headers(),
                timeout=10,
            )
            resp.raise_for_status()

        self._run_action(task, "Користувача видалено")

    def _save_role_password(self, role: str) -> None:
        password = self.password_fields[role].text().strip()
        payload = json.dumps({"password": password})

        def task() -> None:
            resp = requests.post(
                self._api_url(f"/admin/role-passwords/{role}"),
                headers=self._headers(),
                data=payload,
                timeout=10,
            )
            resp.raise_for_status()

        self._run_action(task, "Пароль оновлено")

    def _run_action(self, task: Callable[[], None], success_message: str) -> None:
        worker = Worker(task)
        worker.signals.success.connect(lambda _: self._handle_action_success(success_message))
        worker.signals.error.connect(self._show_error)
        self._thread_pool.start(worker)

    def _handle_action_success(self, message: str) -> None:
        QMessageBox.information(self, "Успішно", message)
        self._load_data()

    def _show_error(self, message: str) -> None:
        QMessageBox.warning(self, "Помилка", message)


class TrackingDashboard(QWidget):
    def __init__(self, settings: Settings, store: OfflineStore, on_logout: Callable[[], None]) -> None:
        super().__init__()
        self.settings = settings
        self.store = store
        self.on_logout = on_logout
        self.thread_pool = QThreadPool.globalInstance()
        self.role_info = parse_user_role(
            settings.get("user_role"), settings.get("access_level")
        )
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        header = QHBoxLayout()
        user_label = QLabel(
            f"Оператор: {self.settings.get('user_name', 'operator')}"
        )
        role_label = QLabel(self.role_info.label)
        role_label.setObjectName("roleLabel")
        header.addWidget(user_label)
        header.addWidget(role_label)
        header.addStretch()
        logout_btn = QPushButton("Вийти")
        logout_btn.clicked.connect(self.on_logout)
        header.addWidget(logout_btn)
        layout.addLayout(header)

        self.tabs = QTabWidget()
        self.scanner_tab = ScannerTab(self.settings, self.store)
        self.history_tab = HistoryTab(self.settings, self.role_info)
        self.errors_tab = ErrorsTab(self.settings, self.role_info)
        self.tabs.addTab(self.scanner_tab, "Сканер")
        self.tabs.addTab(self.history_tab, "Історія")
        self.tabs.addTab(self.errors_tab, "Помилки")
        if self.role_info.is_admin:
            self.stats_tab = StatisticsTab(self.settings)
            self.tabs.addTab(self.stats_tab, "Статистика")
        layout.addWidget(self.tabs)


class ScannerTab(QWidget):
    def __init__(self, settings: Settings, store: OfflineStore) -> None:
        super().__init__()
        self.settings = settings
        self.store = store
        self.thread_pool = QThreadPool.globalInstance()
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        form = QFormLayout()
        self.boxid_input = QLineEdit()
        self.ttn_input = QLineEdit()
        self.boxid_input.returnPressed.connect(self._focus_ttn)
        self.ttn_input.returnPressed.connect(self._send_record)
        form.addRow("BoxID:", self.boxid_input)
        form.addRow("TTN:", self.ttn_input)
        layout.addLayout(form)

        action_layout = QHBoxLayout()
        send_btn = QPushButton("Надіслати")
        send_btn.clicked.connect(self._send_record)
        camera_btn = QPushButton("Сканувати камерою")
        camera_btn.clicked.connect(self._open_manual_scan)
        sync_btn = QPushButton("Синхронізувати офлайн")
        sync_btn.clicked.connect(self._sync_offline)
        action_layout.addWidget(send_btn)
        action_layout.addWidget(camera_btn)
        action_layout.addWidget(sync_btn)
        layout.addLayout(action_layout)

        self.status = QLabel("")
        layout.addWidget(self.status)

    def _sanitize(self, value: str) -> str:
        return "".join(ch for ch in value if ch.isdigit())

    def _focus_ttn(self) -> None:
        self.ttn_input.setFocus()

    def _open_manual_scan(self) -> None:
        text, ok = QInputDialog.getText(self, "Сканування", "Введіть код: ")
        if ok and text:
            sanitized = self._sanitize(text)
            if not self.boxid_input.text().strip():
                self.boxid_input.setText(sanitized)
                self.ttn_input.setFocus()
            else:
                self.ttn_input.setText(sanitized)
                self._send_record()

    def _send_record(self) -> None:
        boxid = self._sanitize(self.boxid_input.text())
        ttn = self._sanitize(self.ttn_input.text())
        if not boxid or not ttn:
            self.status.setText("Заповніть BoxID і TTN")
            return
        token = self.settings.get("token")
        if not token:
            self.status.setText("Відсутній токен. Увійдіть знову.")
            return

        record = {
            "user_name": self.settings.get("user_name", "operator"),
            "boxid": boxid,
            "ttn": ttn,
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

        worker = Worker(task)
        worker.signals.success.connect(lambda _: self._handle_send_success())
        worker.signals.error.connect(lambda msg: self._handle_send_error(msg, record))
        self.thread_pool.start(worker)
        self.status.setText("Надсилання...")

    def _handle_send_success(self) -> None:
        self.status.setText("✅ Запис надіслано")
        self.boxid_input.clear()
        self.ttn_input.clear()
        self.boxid_input.setFocus()
        QApplication.beep()

    def _handle_send_error(self, message: str, record: Dict[str, str]) -> None:
        self.status.setText("⚠️ Немає зв'язку, запис збережено офлайн")
        self.store.add_tracking_record(record)
        QApplication.beep()

    def _sync_offline(self) -> None:
        token = self.settings.get("token")
        if not token:
            self.status.setText("Відсутній токен. Увійдіть знову.")
            return

        def task() -> str:
            records = self.store.list_tracking_records()
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
                self.store.clear_tracking_records(sent_ids)
            return f"Синхронізовано: {len(sent_ids)}"

        worker = Worker(task)
        worker.signals.success.connect(lambda msg: self.status.setText(str(msg)))
        worker.signals.error.connect(lambda msg: self.status.setText(f"Помилка: {msg}"))
        self.thread_pool.start(worker)


class HistoryTab(QWidget):
    def __init__(self, settings: Settings, role_info: UserRoleInfo) -> None:
        super().__init__()
        self.settings = settings
        self.role_info = role_info
        self.thread_pool = QThreadPool.globalInstance()
        self.records: List[Dict[str, Any]] = []
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)

        filters = QGridLayout()
        self.box_filter = QLineEdit()
        self.ttn_filter = QLineEdit()
        self.user_filter = QLineEdit()
        self.date_filter = QDateEdit()
        self.date_filter.setCalendarPopup(True)
        self.date_filter.setDisplayFormat("dd.MM.yyyy")
        self.date_filter.setDate(QDate.currentDate())
        self.date_filter.setSpecialValueText("Будь-яка")
        self.date_filter.setMinimumDate(QDate(2000, 1, 1))
        self.date_filter.setDate(QDate(2000, 1, 1))

        self.start_time = QTimeEdit()
        self.start_time.setDisplayFormat("HH:mm")
        self.start_time.setTime(QTime(0, 0))
        self.end_time = QTimeEdit()
        self.end_time.setDisplayFormat("HH:mm")
        self.end_time.setTime(QTime(23, 59))

        filters.addWidget(QLabel("BoxID"), 0, 0)
        filters.addWidget(self.box_filter, 0, 1)
        filters.addWidget(QLabel("TTN"), 0, 2)
        filters.addWidget(self.ttn_filter, 0, 3)
        filters.addWidget(QLabel("Користувач"), 1, 0)
        filters.addWidget(self.user_filter, 1, 1)
        filters.addWidget(QLabel("Дата"), 1, 2)
        filters.addWidget(self.date_filter, 1, 3)
        filters.addWidget(QLabel("Початок"), 2, 0)
        filters.addWidget(self.start_time, 2, 1)
        filters.addWidget(QLabel("Кінець"), 2, 2)
        filters.addWidget(self.end_time, 2, 3)

        layout.addLayout(filters)

        buttons = QHBoxLayout()
        refresh_btn = QPushButton("Оновити")
        refresh_btn.clicked.connect(self._fetch_history)
        apply_btn = QPushButton("Застосувати фільтри")
        apply_btn.clicked.connect(self._apply_filters)
        clear_btn = QPushButton("Скинути фільтри")
        clear_btn.clicked.connect(self._clear_filters)
        buttons.addWidget(refresh_btn)
        buttons.addWidget(apply_btn)
        buttons.addWidget(clear_btn)

        if self.role_info.can_clear_history:
            clear_history_btn = QPushButton("Очистити історію")
            clear_history_btn.clicked.connect(self._clear_history)
            buttons.addWidget(clear_history_btn)

        layout.addLayout(buttons)

        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["BoxID", "TTN", "Оператор", "Дата"])
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table)

        self._fetch_history()

    def _fetch_history(self) -> None:
        token = self.settings.get("token")
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
                    (item.get("datetime") or "1970-01-01T00:00:00")
                ),
                reverse=True,
            )
            return data

        worker = Worker(task)
        worker.signals.success.connect(self._set_records)
        worker.signals.error.connect(lambda msg: QMessageBox.warning(self, "Помилка", msg))
        self.thread_pool.start(worker)

    def _set_records(self, records: List[Dict[str, Any]]) -> None:
        self.records = records
        self._apply_filters()

    def _apply_filters(self) -> None:
        filtered = self.records[:]
        box_value = self.box_filter.text().strip()
        ttn_value = self.ttn_filter.text().strip()
        user_value = self.user_filter.text().strip().lower()

        if box_value:
            filtered = [item for item in filtered if box_value in str(item.get("boxid", ""))]
        if ttn_value:
            filtered = [item for item in filtered if ttn_value in str(item.get("ttn", ""))]
        if user_value:
            filtered = [
                item
                for item in filtered
                if user_value in str(item.get("user_name", "")).lower()
            ]

        if self.date_filter.date() != QDate(2000, 1, 1):
            selected = self.date_filter.date().toPython()
            filtered = [
                item
                for item in filtered
                if self._parse_date(item.get("datetime"))
                and self._parse_date(item.get("datetime")).date() == selected
            ]

        start_time = self.start_time.time().toPython()
        end_time = self.end_time.time().toPython()

        def within_time(item: Dict[str, Any]) -> bool:
            dt = self._parse_date(item.get("datetime"))
            if not dt:
                return False
            return start_time <= dt.time() <= end_time

        filtered = [item for item in filtered if within_time(item)]
        self._populate_table(filtered)

    def _populate_table(self, records: List[Dict[str, Any]]) -> None:
        self.table.setRowCount(len(records))
        for row, item in enumerate(records):
            self.table.setItem(row, 0, QTableWidgetItem(str(item.get("boxid", ""))))
            self.table.setItem(row, 1, QTableWidgetItem(str(item.get("ttn", ""))))
            self.table.setItem(row, 2, QTableWidgetItem(str(item.get("user_name", ""))))
            self.table.setItem(row, 3, QTableWidgetItem(self._format_date(item.get("datetime"))))

    def _clear_filters(self) -> None:
        self.box_filter.clear()
        self.ttn_filter.clear()
        self.user_filter.clear()
        self.date_filter.setDate(QDate(2000, 1, 1))
        self.start_time.setTime(QTime(0, 0))
        self.end_time.setTime(QTime(23, 59))
        self._apply_filters()

    def _parse_date(self, value: Any) -> Optional[datetime]:
        if isinstance(value, str) and value:
            try:
                return datetime.fromisoformat(value).astimezone()
            except ValueError:
                return None
        return None

    def _format_date(self, value: Any) -> str:
        dt = self._parse_date(value)
        if not dt:
            return str(value or "")
        return dt.strftime("%d.%m.%Y %H:%M:%S")

    def _clear_history(self) -> None:
        if QMessageBox.question(self, "Підтвердження", "Очистити історію?") != QMessageBox.Yes:
            return
        token = self.settings.get("token")
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

        worker = Worker(task)
        worker.signals.success.connect(lambda msg: QMessageBox.information(self, "Готово", msg))
        worker.signals.success.connect(lambda _: self._set_records([]))
        worker.signals.error.connect(lambda msg: QMessageBox.warning(self, "Помилка", msg))
        self.thread_pool.start(worker)


class ErrorsTab(QWidget):
    def __init__(self, settings: Settings, role_info: UserRoleInfo) -> None:
        super().__init__()
        self.settings = settings
        self.role_info = role_info
        self.thread_pool = QThreadPool.globalInstance()
        self._build_ui()
        self._fetch_errors()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        button_row = QHBoxLayout()
        refresh_btn = QPushButton("Оновити")
        refresh_btn.clicked.connect(self._fetch_errors)
        button_row.addWidget(refresh_btn)

        if self.role_info.can_clear_errors:
            clear_btn = QPushButton("Очистити журнал")
            clear_btn.clicked.connect(self._clear_errors)
            delete_btn = QPushButton("Видалити вибраний")
            delete_btn.clicked.connect(self._delete_selected)
            button_row.addWidget(clear_btn)
            button_row.addWidget(delete_btn)

        layout.addLayout(button_row)

        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(["ID", "BoxID", "TTN", "Дата"])
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table)

    def _fetch_errors(self) -> None:
        token = self.settings.get("token")
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
                    (item.get("datetime") or "1970-01-01T00:00:00")
                ),
                reverse=True,
            )
            return data

        worker = Worker(task)
        worker.signals.success.connect(self._populate)
        worker.signals.error.connect(lambda msg: QMessageBox.warning(self, "Помилка", msg))
        self.thread_pool.start(worker)

    def _populate(self, records: List[Dict[str, Any]]) -> None:
        self.table.setRowCount(len(records))
        for row, item in enumerate(records):
            self.table.setItem(row, 0, QTableWidgetItem(str(item.get("id", ""))))
            self.table.setItem(row, 1, QTableWidgetItem(str(item.get("boxid", ""))))
            self.table.setItem(row, 2, QTableWidgetItem(str(item.get("ttn", ""))))
            self.table.setItem(row, 3, QTableWidgetItem(self._format_date(item.get("datetime"))))

    def _format_date(self, value: Any) -> str:
        try:
            return datetime.fromisoformat(value).astimezone().strftime("%d.%m.%Y %H:%M:%S")
        except Exception:
            return str(value or "")

    def _clear_errors(self) -> None:
        if QMessageBox.question(self, "Підтвердження", "Очистити журнал помилок?") != QMessageBox.Yes:
            return
        token = self.settings.get("token")
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

        worker = Worker(task)
        worker.signals.success.connect(lambda msg: QMessageBox.information(self, "Готово", msg))
        worker.signals.success.connect(lambda _: self._fetch_errors())
        worker.signals.error.connect(lambda msg: QMessageBox.warning(self, "Помилка", msg))
        self.thread_pool.start(worker)

    def _delete_selected(self) -> None:
        items = self.table.selectedItems()
        if not items:
            return
        error_id = items[0].text()
        if QMessageBox.question(self, "Підтвердження", f"Видалити помилку #{error_id}?") != QMessageBox.Yes:
            return
        token = self.settings.get("token")
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

        worker = Worker(task)
        worker.signals.success.connect(lambda msg: QMessageBox.information(self, "Готово", msg))
        worker.signals.success.connect(lambda _: self._fetch_errors())
        worker.signals.error.connect(lambda msg: QMessageBox.warning(self, "Помилка", msg))
        self.thread_pool.start(worker)


class StatisticsTab(QWidget):
    def __init__(self, settings: Settings) -> None:
        super().__init__()
        self.settings = settings
        self.thread_pool = QThreadPool.globalInstance()
        self.history: List[Dict[str, Any]] = []
        self.errors: List[Dict[str, Any]] = []
        self._build_ui()
        self._fetch_data()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        filter_layout = QGridLayout()
        self.start_date = QDateEdit(QDate.currentDate().addDays(-7))
        self.start_date.setCalendarPopup(True)
        self.end_date = QDateEdit(QDate.currentDate())
        self.end_date.setCalendarPopup(True)
        self.start_time = QTimeEdit(QTime(0, 0))
        self.end_time = QTimeEdit(QTime(23, 59))
        filter_layout.addWidget(QLabel("Початок"), 0, 0)
        filter_layout.addWidget(self.start_date, 0, 1)
        filter_layout.addWidget(self.start_time, 0, 2)
        filter_layout.addWidget(QLabel("Кінець"), 1, 0)
        filter_layout.addWidget(self.end_date, 1, 1)
        filter_layout.addWidget(self.end_time, 1, 2)
        layout.addLayout(filter_layout)

        refresh_btn = QPushButton("Оновити")
        refresh_btn.clicked.connect(self._fetch_data)
        apply_btn = QPushButton("Застосувати")
        apply_btn.clicked.connect(self._apply_filters)
        btn_layout = QHBoxLayout()
        btn_layout.addWidget(refresh_btn)
        btn_layout.addWidget(apply_btn)
        layout.addLayout(btn_layout)

        self.summary = QTextEdit()
        self.summary.setReadOnly(True)
        layout.addWidget(self.summary)

        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Дата", "Сканів", "Помилок"])
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table)

    def _fetch_data(self) -> None:
        token = self.settings.get("token")
        if not token:
            return

        def task() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
            headers = {"Authorization": f"Bearer {token}"}
            history_resp = requests.get(f"{api_base_url()}/get_history", headers=headers, timeout=10)
            errors_resp = requests.get(f"{api_base_url()}/get_errors", headers=headers, timeout=10)
            history_resp.raise_for_status()
            errors_resp.raise_for_status()
            return history_resp.json(), errors_resp.json()

        worker = Worker(task)
        worker.signals.success.connect(self._set_data)
        worker.signals.error.connect(lambda msg: QMessageBox.warning(self, "Помилка", msg))
        self.thread_pool.start(worker)

    def _set_data(self, data: Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]) -> None:
        self.history, self.errors = data
        self._apply_filters()

    def _apply_filters(self) -> None:
        start_dt = datetime.combine(self.start_date.date().toPython(), self.start_time.time().toPython())
        end_dt = datetime.combine(self.end_date.date().toPython(), self.end_time.time().toPython())
        if start_dt > end_dt:
            start_dt, end_dt = end_dt, start_dt

        history = [item for item in self.history if self._within_range(item, start_dt, end_dt)]
        errors = [item for item in self.errors if self._within_range(item, start_dt, end_dt)]

        scan_counts: Dict[str, int] = {}
        error_counts: Dict[str, int] = {}
        daily_scans: Dict[date, int] = {}
        daily_errors: Dict[date, int] = {}

        for item in history:
            user = str(item.get("user_name", "—"))
            scan_counts[user] = scan_counts.get(user, 0) + 1
            dt = self._parse_date(item.get("datetime"))
            if dt:
                daily_scans[dt.date()] = daily_scans.get(dt.date(), 0) + 1

        for item in errors:
            user = str(item.get("user_name", "—"))
            error_counts[user] = error_counts.get(user, 0) + 1
            dt = self._parse_date(item.get("datetime"))
            if dt:
                daily_errors[dt.date()] = daily_errors.get(dt.date(), 0) + 1

        total_scans = sum(scan_counts.values())
        total_errors = sum(error_counts.values())
        top_user, top_count = self._top_item(scan_counts)
        top_error_user, top_error_count = self._top_item(error_counts)

        summary = (
            f"Усього сканів: {total_scans}\n"
            f"Унікальних операторів: {len(scan_counts)}\n"
            f"Усього помилок: {total_errors}\n"
            f"Оператор з найбільшою активністю: {top_user} ({top_count})\n"
            f"Оператор з найбільшою кількістю помилок: {top_error_user} ({top_error_count})\n"
        )
        self.summary.setText(summary)

        all_dates = sorted(set(daily_scans.keys()) | set(daily_errors.keys()))
        self.table.setRowCount(len(all_dates))
        for row, day in enumerate(all_dates):
            self.table.setItem(row, 0, QTableWidgetItem(day.strftime("%d.%m.%Y")))
            self.table.setItem(row, 1, QTableWidgetItem(str(daily_scans.get(day, 0))))
            self.table.setItem(row, 2, QTableWidgetItem(str(daily_errors.get(day, 0))))

    def _within_range(self, item: Dict[str, Any], start: datetime, end: datetime) -> bool:
        dt = self._parse_date(item.get("datetime"))
        if not dt:
            return False
        return start <= dt <= end

    def _parse_date(self, value: Any) -> Optional[datetime]:
        if isinstance(value, str) and value:
            try:
                return datetime.fromisoformat(value).astimezone()
            except ValueError:
                return None
        return None

    def _top_item(self, data: Dict[str, int]) -> Tuple[str, int]:
        if not data:
            return "—", 0
        top_user = max(data.items(), key=lambda item: item[1])
        return top_user[0], top_user[1]


class ScanpakHome(QWidget):
    def __init__(self, settings: Settings, store: OfflineStore, on_logout: Callable[[], None]) -> None:
        super().__init__()
        self.settings = settings
        self.store = store
        self.on_logout = on_logout
        self.thread_pool = QThreadPool.globalInstance()
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        header = QHBoxLayout()
        header.addWidget(QLabel(f"Оператор: {self.settings.get('scanpak_user_name', '—')}") )
        header.addStretch()
        logout_btn = QPushButton("Вийти")
        logout_btn.clicked.connect(self.on_logout)
        header.addWidget(logout_btn)
        layout.addLayout(header)

        self.tabs = QTabWidget()
        self.scan_tab = ScanpakScanTab(self.settings, self.store)
        self.history_tab = ScanpakHistoryTab(self.settings)
        self.stats_tab = ScanpakStatsTab(self.settings)
        self.tabs.addTab(self.scan_tab, "Сканування")
        self.tabs.addTab(self.history_tab, "Історія")
        self.tabs.addTab(self.stats_tab, "Статистика")
        layout.addWidget(self.tabs)


class ScanpakScanTab(QWidget):
    def __init__(self, settings: Settings, store: OfflineStore) -> None:
        super().__init__()
        self.settings = settings
        self.store = store
        self.thread_pool = QThreadPool.globalInstance()
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        form = QFormLayout()
        self.number_input = QLineEdit()
        self.number_input.returnPressed.connect(self._send_scan)
        form.addRow("Номер відправлення:", self.number_input)
        layout.addLayout(form)

        buttons = QHBoxLayout()
        send_btn = QPushButton("Надіслати")
        send_btn.clicked.connect(self._send_scan)
        sync_btn = QPushButton("Синхронізувати офлайн")
        sync_btn.clicked.connect(self._sync_offline)
        buttons.addWidget(send_btn)
        buttons.addWidget(sync_btn)
        layout.addLayout(buttons)

        self.status = QLabel("")
        layout.addWidget(self.status)

    def _sanitize(self, value: str) -> str:
        return "".join(ch for ch in value if ch.isdigit())

    def _send_scan(self) -> None:
        token = self.settings.get("scanpak_token")
        if not token:
            self.status.setText("Відсутній токен. Увійдіть знову.")
            return
        digits = self._sanitize(self.number_input.text())
        if not digits:
            self.status.setText("Введіть номер відправлення")
            return

        payload = json.dumps({"parcel_number": digits})

        def task() -> str:
            resp = requests.post(
                f"{scanpak_base_url()}/scans",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                data=payload,
                timeout=10,
            )
            if resp.status_code == 200:
                return "ok"
            raise RuntimeError(resp.text)

        worker = Worker(task)
        worker.signals.success.connect(lambda _: self._handle_success())
        worker.signals.error.connect(lambda msg: self._handle_error(msg, digits))
        self.thread_pool.start(worker)
        self.status.setText("Надсилання...")

    def _handle_success(self) -> None:
        self.status.setText("✅ Скан збережено")
        self.number_input.clear()
        self.number_input.setFocus()
        QApplication.beep()

    def _handle_error(self, message: str, digits: str) -> None:
        self.status.setText("⚠️ Немає зв'язку, запис збережено офлайн")
        if not self.store.scanpak_contains(digits):
            self.store.add_scanpak_record(digits)
        QApplication.beep()

    def _sync_offline(self) -> None:
        token = self.settings.get("scanpak_token")
        if not token:
            self.status.setText("Відсутній токен. Увійдіть знову.")
            return

        def task() -> str:
            records = self.store.list_scanpak_records()
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
                    self.store.remove_scanpak_record(int(record["id"]))
            return f"Синхронізовано: {sent}"

        worker = Worker(task)
        worker.signals.success.connect(lambda msg: self.status.setText(str(msg)))
        worker.signals.error.connect(lambda msg: self.status.setText(f"Помилка: {msg}"))
        self.thread_pool.start(worker)


class ScanpakHistoryTab(QWidget):
    def __init__(self, settings: Settings) -> None:
        super().__init__()
        self.settings = settings
        self.thread_pool = QThreadPool.globalInstance()
        self.records: List[Dict[str, Any]] = []
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        filter_layout = QGridLayout()
        self.parcel_filter = QLineEdit()
        self.user_filter = QLineEdit()
        self.date_filter = QDateEdit(QDate.currentDate())
        self.date_filter.setCalendarPopup(True)
        self.date_filter.setDate(QDate(2000, 1, 1))
        filter_layout.addWidget(QLabel("Номер"), 0, 0)
        filter_layout.addWidget(self.parcel_filter, 0, 1)
        filter_layout.addWidget(QLabel("Користувач"), 0, 2)
        filter_layout.addWidget(self.user_filter, 0, 3)
        filter_layout.addWidget(QLabel("Дата"), 1, 0)
        filter_layout.addWidget(self.date_filter, 1, 1)
        layout.addLayout(filter_layout)

        buttons = QHBoxLayout()
        refresh_btn = QPushButton("Оновити")
        refresh_btn.clicked.connect(self._fetch_history)
        apply_btn = QPushButton("Застосувати")
        apply_btn.clicked.connect(self._apply_filters)
        buttons.addWidget(refresh_btn)
        buttons.addWidget(apply_btn)
        layout.addLayout(buttons)

        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Номер", "Користувач", "Дата"])
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table)

        self._fetch_history()

    def _fetch_history(self) -> None:
        token = self.settings.get("scanpak_token")
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
                    (item.get("created_at") or "1970-01-01T00:00:00")
                ),
                reverse=True,
            )
            return data

        worker = Worker(task)
        worker.signals.success.connect(self._set_records)
        worker.signals.error.connect(lambda msg: QMessageBox.warning(self, "Помилка", msg))
        self.thread_pool.start(worker)

    def _set_records(self, records: List[Dict[str, Any]]) -> None:
        self.records = records
        self._apply_filters()

    def _apply_filters(self) -> None:
        filtered = self.records[:]
        parcel_value = self.parcel_filter.text().strip()
        user_value = self.user_filter.text().strip().lower()
        if parcel_value:
            filtered = [item for item in filtered if parcel_value in str(item.get("parcel_number", ""))]
        if user_value:
            filtered = [
                item
                for item in filtered
                if user_value in str(item.get("user_name", "")).lower()
            ]
        if self.date_filter.date() != QDate(2000, 1, 1):
            target = self.date_filter.date().toPython()
            filtered = [
                item
                for item in filtered
                if self._parse_date(item.get("created_at"))
                and self._parse_date(item.get("created_at")).date() == target
            ]
        self.table.setRowCount(len(filtered))
        for row, item in enumerate(filtered):
            self.table.setItem(row, 0, QTableWidgetItem(str(item.get("parcel_number", ""))))
            self.table.setItem(row, 1, QTableWidgetItem(str(item.get("user_name", ""))))
            self.table.setItem(row, 2, QTableWidgetItem(self._format_date(item.get("created_at"))))

    def _parse_date(self, value: Any) -> Optional[datetime]:
        if isinstance(value, str) and value:
            try:
                return datetime.fromisoformat(value).astimezone()
            except ValueError:
                return None
        return None

    def _format_date(self, value: Any) -> str:
        dt = self._parse_date(value)
        return dt.strftime("%d.%m.%Y %H:%M:%S") if dt else str(value or "")


class ScanpakStatsTab(QWidget):
    def __init__(self, settings: Settings) -> None:
        super().__init__()
        self.settings = settings
        self.thread_pool = QThreadPool.globalInstance()
        self.records: List[Dict[str, Any]] = []
        self._build_ui()
        self._fetch_history()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        self.start_date = QDateEdit(QDate.currentDate().addDays(-7))
        self.start_date.setCalendarPopup(True)
        self.end_date = QDateEdit(QDate.currentDate())
        self.end_date.setCalendarPopup(True)
        self.user_filter = QLineEdit()

        form = QFormLayout()
        form.addRow("Початок", self.start_date)
        form.addRow("Кінець", self.end_date)
        form.addRow("Користувач", self.user_filter)
        layout.addLayout(form)

        buttons = QHBoxLayout()
        refresh_btn = QPushButton("Оновити")
        refresh_btn.clicked.connect(self._fetch_history)
        apply_btn = QPushButton("Застосувати")
        apply_btn.clicked.connect(self._apply_filters)
        buttons.addWidget(refresh_btn)
        buttons.addWidget(apply_btn)
        layout.addLayout(buttons)

        self.summary = QTextEdit()
        self.summary.setReadOnly(True)
        layout.addWidget(self.summary)

    def _fetch_history(self) -> None:
        token = self.settings.get("scanpak_token")
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

        worker = Worker(task)
        worker.signals.success.connect(self._set_records)
        worker.signals.error.connect(lambda msg: QMessageBox.warning(self, "Помилка", msg))
        self.thread_pool.start(worker)

    def _set_records(self, records: List[Dict[str, Any]]) -> None:
        self.records = records
        self._apply_filters()

    def _apply_filters(self) -> None:
        start = self.start_date.date().toPython()
        end = self.end_date.date().toPython()
        if start > end:
            start, end = end, start
        user_filter = self.user_filter.text().strip().lower()

        filtered: List[Dict[str, Any]] = []
        for item in self.records:
            dt = self._parse_date(item.get("created_at"))
            if not dt:
                continue
            if not (start <= dt.date() <= end):
                continue
            if user_filter and user_filter not in str(item.get("user_name", "")).lower():
                continue
            filtered.append(item)

        per_user: Dict[str, int] = {}
        for item in filtered:
            user = str(item.get("user_name", "—"))
            per_user[user] = per_user.get(user, 0) + 1

        top_user, top_count = self._top_item(per_user)
        summary = (
            f"Сканів у періоді: {len(filtered)}\n"
            f"Унікальних користувачів: {len(per_user)}\n"
            f"Найактивніший оператор: {top_user} ({top_count})\n"
        )
        self.summary.setText(summary)

    def _parse_date(self, value: Any) -> Optional[datetime]:
        if isinstance(value, str) and value:
            try:
                return datetime.fromisoformat(value).astimezone()
            except ValueError:
                return None
        return None

    def _top_item(self, data: Dict[str, int]) -> Tuple[str, int]:
        if not data:
            return "—", 0
        top_user = max(data.items(), key=lambda item: item[1])
        return top_user[0], top_user[1]


class TrackingApp(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.settings = Settings()
        self.store = OfflineStore()
        self.thread_pool = QThreadPool.globalInstance()
        self.setWindowTitle(APP_NAME)
        self.resize(1100, 700)
        self._build_ui()
        self._apply_theme()

    def _build_ui(self) -> None:
        self.stack = QStackedWidget()
        self.setCentralWidget(self.stack)

        self.start_page = StartPage(self._navigate)
        self.tracking_login = LoginPage(
            "TrackingApp",
            self._tracking_login,
            self._tracking_register,
            on_admin=self._open_admin_panel,
        )
        self.scanpak_login = LoginPage(
            "СканПак",
            self._scanpak_login,
            self._scanpak_register,
            on_admin=self._open_scanpak_admin_panel,
        )

        self.stack.addWidget(self.start_page)
        self.stack.addWidget(self.tracking_login)
        self.stack.addWidget(self.scanpak_login)

        self._navigate("start")

    def _apply_theme(self) -> None:
        self.setStyleSheet(
            """
            QWidget { font-family: 'Segoe UI'; font-size: 14px; }
            QLabel#title { font-size: 28px; font-weight: 600; }
            QLabel[success="true"] { color: #2e7d32; }
            QLabel { color: #0f172a; }
            QPushButton { background: #2563eb; color: white; padding: 8px 16px; border-radius: 6px; }
            QPushButton:hover { background: #1d4ed8; }
            QPushButton:disabled { background: #94a3b8; }
            QTabBar::tab { padding: 8px 16px; }
            QLineEdit, QDateEdit, QTimeEdit { padding: 6px; border: 1px solid #cbd5f5; border-radius: 6px; }
            QTextEdit { border: 1px solid #cbd5f5; border-radius: 6px; padding: 8px; }
            #roleLabel { font-weight: 600; color: #ef4444; }
            """
        )

    def _navigate(self, target: str) -> None:
        if target == "start":
            self.stack.setCurrentWidget(self.start_page)
        elif target == "tracking_login":
            self.tracking_login.reset()
            self.stack.setCurrentWidget(self.tracking_login)
        elif target == "scanpak_login":
            self.scanpak_login.reset()
            self.stack.setCurrentWidget(self.scanpak_login)

    def _tracking_login(self, surname: str, password: str) -> None:
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

        worker = Worker(task)
        worker.signals.success.connect(self._handle_tracking_login)
        worker.signals.error.connect(lambda msg: self.tracking_login.set_status(msg))
        self.thread_pool.start(worker)
        self.tracking_login.set_status("Вхід...", success=True)

    def _handle_tracking_login(self, data: Dict[str, Any]) -> None:
        token = str(data.get("token", ""))
        if not token:
            self.tracking_login.set_status("Сервер не повернув токен")
            return
        self.settings.set("token", token)
        self.settings.set("user_name", data.get("surname") or "operator")
        self.settings.set("access_level", data.get("access_level"))
        self.settings.set("user_role", data.get("role"))
        self._open_tracking_dashboard()

    def _tracking_register(self, surname: str, password: str) -> None:
        def task() -> None:
            resp = requests.post(
                f"{api_base_url()}/register",
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                data=json.dumps({"surname": surname, "password": password}),
                timeout=10,
            )
            if resp.status_code != 200:
                raise RuntimeError(resp.text)

        worker = Worker(task)
        worker.signals.success.connect(
            lambda _: self.tracking_login.set_status(
                "Заявку на реєстрацію відправлено", success=True
            )
        )
        worker.signals.error.connect(lambda msg: self.tracking_login.set_status(msg))
        self.thread_pool.start(worker)

    def _open_tracking_dashboard(self) -> None:
        self.dashboard = TrackingDashboard(self.settings, self.store, self._logout_tracking)
        self.stack.addWidget(self.dashboard)
        self.stack.setCurrentWidget(self.dashboard)

    def _logout_tracking(self) -> None:
        self.settings.clear(["token", "access_level", "user_name", "user_role"])
        self._navigate("start")

    def _scanpak_login(self, surname: str, password: str) -> None:
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

        worker = Worker(task)
        worker.signals.success.connect(self._handle_scanpak_login)
        worker.signals.error.connect(lambda msg: self.scanpak_login.set_status(msg))
        self.thread_pool.start(worker)
        self.scanpak_login.set_status("Вхід...", success=True)

    def _handle_scanpak_login(self, data: Dict[str, Any]) -> None:
        token = str(data.get("token", ""))
        if not token:
            self.scanpak_login.set_status("Сервер не повернув токен")
            return
        self.settings.set("scanpak_token", token)
        self.settings.set("scanpak_user_name", data.get("surname") or "operator")
        self.settings.set("scanpak_user_role", data.get("role"))
        self._open_scanpak_home()

    def _scanpak_register(self, surname: str, password: str) -> None:
        def task() -> None:
            resp = requests.post(
                f"{scanpak_base_url()}/register",
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                data=json.dumps({"surname": surname, "password": password}),
                timeout=10,
            )
            if resp.status_code != 200:
                raise RuntimeError(resp.text)

        worker = Worker(task)
        worker.signals.success.connect(
            lambda _: self.scanpak_login.set_status(
                "Заявку на реєстрацію відправлено", success=True
            )
        )
        worker.signals.error.connect(lambda msg: self.scanpak_login.set_status(msg))
        self.thread_pool.start(worker)

    def _open_scanpak_home(self) -> None:
        self.scanpak_home = ScanpakHome(self.settings, self.store, self._logout_scanpak)
        self.stack.addWidget(self.scanpak_home)
        self.stack.setCurrentWidget(self.scanpak_home)

    def _logout_scanpak(self) -> None:
        self.settings.clear(["scanpak_token", "scanpak_user_name", "scanpak_user_role"])
        self._navigate("start")

    def _open_admin_panel(self) -> None:
        password, ok = QInputDialog.getText(self, "Адмін панель", "Пароль адміністратора:")
        if not ok or not password.strip():
            return

        def task() -> Dict[str, Any]:
            resp = requests.post(
                f"{api_base_url()}/admin_login",
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                data=json.dumps({"password": password.strip()}),
                timeout=10,
            )
            if resp.status_code != 200:
                raise RuntimeError(resp.text)
            return resp.json()

        worker = Worker(task)
        worker.signals.success.connect(self._launch_admin_panel)
        worker.signals.error.connect(lambda msg: QMessageBox.warning(self, "Помилка", msg))
        self.thread_pool.start(worker)

    def _launch_admin_panel(self, data: Dict[str, Any]) -> None:
        token = data.get("token")
        if not token:
            QMessageBox.warning(self, "Помилка", "Сервер не повернув токен")
            return
        dialog = AdminPanelDialog(token, is_scanpak=False, parent=self)
        dialog.exec()

    def _open_scanpak_admin_panel(self) -> None:
        password, ok = QInputDialog.getText(self, "Адмін панель СканПак", "Пароль адміністратора:")
        if not ok or not password.strip():
            return

        def task() -> Dict[str, Any]:
            resp = requests.post(
                f"{scanpak_base_url()}/admin_login",
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                data=json.dumps({"password": password.strip()}),
                timeout=10,
            )
            if resp.status_code != 200:
                raise RuntimeError(resp.text)
            return resp.json()

        worker = Worker(task)
        worker.signals.success.connect(self._launch_scanpak_admin_panel)
        worker.signals.error.connect(lambda msg: QMessageBox.warning(self, "Помилка", msg))
        self.thread_pool.start(worker)

    def _launch_scanpak_admin_panel(self, data: Dict[str, Any]) -> None:
        token = data.get("token")
        if not token:
            QMessageBox.warning(self, "Помилка", "Сервер не повернув токен")
            return
        dialog = AdminPanelDialog(token, is_scanpak=True, parent=self)
        dialog.exec()


def main() -> None:
    app = QApplication(sys.argv)
    window = TrackingApp()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
