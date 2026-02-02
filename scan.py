import datetime as dt
import json
import tkinter as tk
from dataclasses import dataclass
from tkinter import messagebox, ttk
from typing import List

import requests

API_HOST = "173.242.53.38"
API_PORT = 10000
API_BASE_PATH = "/scanpak"


@dataclass(frozen=True)
class ScanRecord:
    number: str
    user: str
    timestamp: dt.datetime

    @staticmethod
    def from_json(payload: dict) -> "ScanRecord":
        number = str(payload.get("parcel_number") or "").strip()
        user = str(payload.get("username") or "").strip()
        raw_time = str(payload.get("scanned_at") or "").strip()
        if not number:
            raise ValueError("Некоректні дані сканування")
        return ScanRecord(number=number, user=user, timestamp=parse_timestamp(raw_time))


def parse_timestamp(raw: str) -> dt.datetime:
    if not raw:
        raise ValueError("Некоректні дані сканування")
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("Некоректні дані сканування") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone()


class ScanpakApi:
    def __init__(self, host: str, port: int, base_path: str) -> None:
        self.host = host
        self.port = port
        self.base_path = base_path

    def _url(self, path: str) -> str:
        return f"http://{self.host}:{self.port}{self.base_path}{path}"

    def login(self, surname: str, password: str) -> dict:
        response = requests.post(
            self._url("/login"),
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            data=json.dumps({"surname": surname, "password": password}),
            timeout=10,
        )
        if response.status_code != 200:
            raise RuntimeError(self._extract_message(response))
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError("Неправильна відповідь сервера")
        token = str(data.get("token") or "").strip()
        if not token:
            raise RuntimeError("Сервер не повернув коректний токен")
        return data

    def fetch_history(self, token: str) -> List[ScanRecord]:
        response = requests.get(
            self._url("/history"),
            headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
            timeout=15,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Не вдалося отримати історію ({response.status_code})"
            )
        payload = response.json()
        if not isinstance(payload, list):
            return []
        records = []
        for item in payload:
            if isinstance(item, dict):
                try:
                    records.append(ScanRecord.from_json(item))
                except ValueError:
                    continue
        return records

    def send_scan(self, token: str, parcel_number: str) -> ScanRecord:
        response = requests.post(
            self._url("/scans"),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
            data=json.dumps({"parcel_number": parcel_number}),
            timeout=10,
        )
        if response.status_code == 401:
            raise RuntimeError("Сесію завершено. Увійдіть знову")
        if response.status_code != 200:
            raise RuntimeError(f"Не вдалося зберегти: {response.status_code}")
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("Некоректна відповідь сервера")
        return ScanRecord.from_json(payload)

    @staticmethod
    def _extract_message(response: requests.Response) -> str:
        try:
            payload = response.json()
            if isinstance(payload, dict):
                detail = payload.get("detail") or payload.get("message")
                if isinstance(detail, str) and detail.strip():
                    return detail
        except json.JSONDecodeError:
            pass
        return f"Помилка ({response.status_code})"


class LoginFrame(ttk.Frame):
    def __init__(self, master: tk.Tk, api: ScanpakApi, on_success) -> None:
        super().__init__(master)
        self.api = api
        self.on_success = on_success
        self._build_ui()

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        title = ttk.Label(self, text="ScanPak — Вхід", font=("Segoe UI", 16, "bold"))
        title.grid(row=0, column=0, pady=(20, 10))

        form = ttk.Frame(self)
        form.grid(row=1, column=0, pady=10, padx=40, sticky="ew")
        form.columnconfigure(1, weight=1)

        ttk.Label(form, text="Прізвище:").grid(row=0, column=0, sticky="w")
        self.surname_entry = ttk.Entry(form)
        self.surname_entry.grid(row=0, column=1, sticky="ew", pady=4)

        ttk.Label(form, text="Пароль:").grid(row=1, column=0, sticky="w")
        self.password_entry = ttk.Entry(form, show="*")
        self.password_entry.grid(row=1, column=1, sticky="ew", pady=4)

        self.status_label = ttk.Label(form, text="", foreground="#b00020")
        self.status_label.grid(row=2, column=0, columnspan=2, sticky="w")

        self.login_button = ttk.Button(form, text="Увійти", command=self._handle_login)
        self.login_button.grid(row=3, column=0, columnspan=2, pady=(10, 0))

        self.surname_entry.focus()
        self.password_entry.bind("<Return>", lambda _: self._handle_login())

    def _handle_login(self) -> None:
        surname = self.surname_entry.get().strip()
        password = self.password_entry.get().strip()

        if not surname or not password:
            self.status_label.config(text="Введіть прізвище та пароль")
            return

        self.login_button.config(state="disabled")
        self.status_label.config(text="")
        self.update_idletasks()

        try:
            data = self.api.login(surname, password)
        except (requests.RequestException, RuntimeError) as exc:
            self.status_label.config(text=str(exc))
            self.login_button.config(state="normal")
            return

        self.login_button.config(state="normal")
        self.on_success(
            token=str(data.get("token")),
            surname=str(data.get("surname") or surname),
            role=str(data.get("role") or ""),
        )


class MainFrame(ttk.Frame):
    def __init__(self, master: tk.Tk, api: ScanpakApi, session: dict, on_logout) -> None:
        super().__init__(master)
        self.api = api
        self.session = session
        self.on_logout = on_logout
        self.records: List[ScanRecord] = []
        self.filtered: List[ScanRecord] = []
        self._build_ui()
        self._refresh_history()

    def _build_ui(self) -> None:
        header = ttk.Frame(self)
        header.pack(fill="x", padx=16, pady=12)

        user_label = ttk.Label(
            header,
            text=f"Користувач: {self.session.get('surname', '')}",
            font=("Segoe UI", 10, "bold"),
        )
        user_label.pack(side="left")

        logout_button = ttk.Button(header, text="Вийти", command=self._logout)
        logout_button.pack(side="right")

        self.status_var = tk.StringVar(value="")
        status_label = ttk.Label(self, textvariable=self.status_var, foreground="#00695c")
        status_label.pack(fill="x", padx=16)

        notebook = ttk.Notebook(self)
        notebook.pack(fill="both", expand=True, padx=16, pady=16)

        self.scan_tab = ttk.Frame(notebook)
        self.history_tab = ttk.Frame(notebook)
        notebook.add(self.scan_tab, text="Сканування")
        notebook.add(self.history_tab, text="Історія")

        self._build_scan_tab()
        self._build_history_tab()

    def _build_scan_tab(self) -> None:
        container = ttk.Frame(self.scan_tab)
        container.pack(fill="both", expand=True, padx=20, pady=20)
        container.columnconfigure(1, weight=1)

        ttk.Label(container, text="Box ID:").grid(row=0, column=0, sticky="w")
        self.scan_entry = ttk.Entry(container)
        self.scan_entry.grid(row=0, column=1, sticky="ew", pady=6)
        self.scan_entry.bind("<Return>", lambda _: self._handle_scan())

        send_button = ttk.Button(container, text="Надіслати", command=self._handle_scan)
        send_button.grid(row=1, column=0, columnspan=2, pady=10)

        self.scan_feedback = ttk.Label(container, text="", foreground="#1b5e20")
        self.scan_feedback.grid(row=2, column=0, columnspan=2, sticky="w")

    def _build_history_tab(self) -> None:
        filters = ttk.LabelFrame(self.history_tab, text="Фільтри")
        filters.pack(fill="x", padx=10, pady=10)
        filters.columnconfigure(1, weight=1)
        filters.columnconfigure(3, weight=1)
        filters.columnconfigure(5, weight=1)

        ttk.Label(filters, text="Box ID:").grid(row=0, column=0, padx=6, pady=6, sticky="w")
        self.filter_box_entry = ttk.Entry(filters)
        self.filter_box_entry.grid(row=0, column=1, padx=6, pady=6, sticky="ew")

        ttk.Label(filters, text="Користувач:").grid(
            row=0, column=2, padx=6, pady=6, sticky="w"
        )
        self.filter_user_entry = ttk.Entry(filters)
        self.filter_user_entry.grid(row=0, column=3, padx=6, pady=6, sticky="ew")

        ttk.Label(filters, text="Дата (YYYY-MM-DD):").grid(
            row=0, column=4, padx=6, pady=6, sticky="w"
        )
        self.filter_date_entry = ttk.Entry(filters)
        self.filter_date_entry.grid(row=0, column=5, padx=6, pady=6, sticky="ew")

        filter_button = ttk.Button(filters, text="Застосувати", command=self._apply_filters)
        filter_button.grid(row=1, column=0, padx=6, pady=6, sticky="w")
        clear_button = ttk.Button(filters, text="Очистити", command=self._clear_filters)
        clear_button.grid(row=1, column=1, padx=6, pady=6, sticky="w")
        refresh_button = ttk.Button(filters, text="Оновити", command=self._refresh_history)
        refresh_button.grid(row=1, column=2, padx=6, pady=6, sticky="w")

        self.history_tree = ttk.Treeview(
            self.history_tab,
            columns=("number", "user", "time"),
            show="headings",
            height=15,
        )
        self.history_tree.heading("number", text="Box ID")
        self.history_tree.heading("user", text="Користувач")
        self.history_tree.heading("time", text="Час")
        self.history_tree.column("number", width=150)
        self.history_tree.column("user", width=150)
        self.history_tree.column("time", width=200)
        self.history_tree.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    def _logout(self) -> None:
        self.on_logout()

    def _apply_filters(self) -> None:
        box_filter = self.filter_box_entry.get().strip()
        user_filter = self.filter_user_entry.get().strip().lower()
        date_filter = self.filter_date_entry.get().strip()

        filtered = list(self.records)
        if box_filter:
            filtered = [
                record for record in filtered if box_filter in record.number
            ]
        if user_filter:
            filtered = [
                record
                for record in filtered
                if user_filter in record.user.lower()
            ]
        if date_filter:
            try:
                target = dt.datetime.strptime(date_filter, "%Y-%m-%d").date()
                filtered = [
                    record
                    for record in filtered
                    if record.timestamp.date() == target
                ]
            except ValueError:
                messagebox.showwarning("Фільтр", "Невірний формат дати")

        self.filtered = filtered
        self._render_history()

    def _clear_filters(self) -> None:
        self.filter_box_entry.delete(0, tk.END)
        self.filter_user_entry.delete(0, tk.END)
        self.filter_date_entry.delete(0, tk.END)
        self.filtered = list(self.records)
        self._render_history()

    def _refresh_history(self) -> None:
        token = self.session.get("token")
        if not token:
            return
        self.status_var.set("Оновлюємо історію...")
        self.update_idletasks()
        try:
            self.records = self.api.fetch_history(token)
            self.filtered = list(self.records)
            self._render_history()
            self.status_var.set("Історія оновлена")
        except (requests.RequestException, RuntimeError) as exc:
            self.status_var.set(str(exc))

    def _render_history(self) -> None:
        self.history_tree.delete(*self.history_tree.get_children())
        for record in self.filtered:
            display_time = record.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            self.history_tree.insert(
                "", tk.END, values=(record.number, record.user, display_time)
            )

    def _handle_scan(self) -> None:
        raw = self.scan_entry.get().strip()
        digits = "".join(ch for ch in raw if ch.isdigit())
        if not digits:
            self.scan_feedback.config(text="Не знайшли цифр у введенні")
            self.scan_entry.focus()
            return

        if self._is_duplicate(digits):
            self.scan_feedback.config(text="Увага, це дублікат. Не збережено")
            self.scan_entry.delete(0, tk.END)
            self.scan_entry.focus()
            return

        token = self.session.get("token")
        if not token:
            messagebox.showwarning("Сесія", "Сесію завершено. Увійдіть знову")
            self.on_logout()
            return

        self.scan_feedback.config(text="Відправляємо...")
        self.update_idletasks()
        try:
            record = self.api.send_scan(token, digits)
        except (requests.RequestException, RuntimeError) as exc:
            messagebox.showerror("Сканування", str(exc))
            self.scan_feedback.config(text="Помилка збереження")
            return

        self.records.insert(0, record)
        self.filtered = list(self.records)
        self._render_history()
        self.scan_feedback.config(
            text=f"Збережено для {record.user} о {record.timestamp.strftime('%H:%M')}"
        )
        self.scan_entry.delete(0, tk.END)
        self.scan_entry.focus()

    def _is_duplicate(self, digits: str) -> bool:
        return any(record.number == digits for record in self.records)


class ScanpakApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("ScanPak для Windows")
        self.geometry("820x620")
        self.minsize(720, 520)

        self.api = ScanpakApi(API_HOST, API_PORT, API_BASE_PATH)
        self.session: dict = {}

        self.container = ttk.Frame(self)
        self.container.pack(fill="both", expand=True)

        self.login_frame = LoginFrame(self.container, self.api, self._handle_login)
        self.main_frame = None
        self.login_frame.pack(fill="both", expand=True)

    def _handle_login(self, token: str, surname: str, role: str) -> None:
        self.session = {"token": token, "surname": surname, "role": role}
        self.login_frame.pack_forget()
        self.main_frame = MainFrame(
            self.container, self.api, self.session, self._handle_logout
        )
        self.main_frame.pack(fill="both", expand=True)

    def _handle_logout(self) -> None:
        if self.main_frame is not None:
            self.main_frame.pack_forget()
            self.main_frame.destroy()
            self.main_frame = None
        self.session = {}
        self.login_frame = LoginFrame(self.container, self.api, self._handle_login)
        self.login_frame.pack(fill="both", expand=True)


def main() -> None:
    app = ScanpakApp()
    app.mainloop()


if __name__ == "__main__":
    main()
