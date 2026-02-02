import datetime as dt
import json
import tkinter as tk
from dataclasses import dataclass
from tkinter import messagebox, ttk
from typing import List, Optional

import requests

API_HOST = "173.242.53.38"
API_PORT = 10000
API_BASE_PATH = "/scanpak"

# ══════════════════════════════════════════════════════════════════════════════
# ЦВЕТОВАЯ СХЕМА И КОНСТАНТЫ ДИЗАЙНА
# ══════════════════════════════════════════════════════════════════════════════

class Colors:
    # Основные цвета
    PRIMARY = "#1a73e8"
    PRIMARY_DARK = "#1557b0"
    PRIMARY_LIGHT = "#4285f4"
    
    # Фоновые цвета
    BG_DARK = "#1e1e2e"
    BG_MEDIUM = "#2d2d44"
    BG_LIGHT = "#383850"
    BG_CARD = "#3d3d5c"
    
    # Текст
    TEXT_PRIMARY = "#ffffff"
    TEXT_SECONDARY = "#b4b4c4"
    TEXT_MUTED = "#8888a0"
    
    # Акценты
    SUCCESS = "#00c853"
    SUCCESS_DARK = "#00a844"
    WARNING = "#ffab00"
    ERROR = "#ff5252"
    ERROR_DARK = "#d32f2f"
    
    # Границы и разделители
    BORDER = "#4a4a6a"
    BORDER_LIGHT = "#5a5a7a"
    
    # Hover эффекты
    HOVER = "#4a4a6a"
    PRESSED = "#5a5a7a"


class Fonts:
    TITLE_LARGE = ("Segoe UI", 28, "bold")
    TITLE = ("Segoe UI", 20, "bold")
    SUBTITLE = ("Segoe UI", 14, "bold")
    BODY = ("Segoe UI", 11)
    BODY_BOLD = ("Segoe UI", 11, "bold")
    SMALL = ("Segoe UI", 10)
    BUTTON = ("Segoe UI", 11, "bold")
    INPUT = ("Segoe UI", 12)


# ══════════════════════════════════════════════════════════════════════════════
# МОДЕЛИ ДАННЫХ
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# API КЛИЕНТ
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# КАСТОМНЫЕ ВИДЖЕТЫ
# ══════════════════════════════════════════════════════════════════════════════

class ModernButton(tk.Canvas):
    """Современная кнопка с эффектами hover и нажатия"""
    
    def __init__(
        self,
        master,
        text: str,
        command=None,
        width: int = 140,
        height: int = 42,
        bg_color: str = Colors.PRIMARY,
        hover_color: str = Colors.PRIMARY_DARK,
        text_color: str = Colors.TEXT_PRIMARY,
        corner_radius: int = 8,
        **kwargs
    ):
        super().__init__(
            master,
            width=width,
            height=height,
            bg=master.cget("bg") if hasattr(master, "cget") else Colors.BG_DARK,
            highlightthickness=0,
            **kwargs
        )
        
        self.command = command
        self.text = text
        self.width = width
        self.height = height
        self.bg_color = bg_color
        self.hover_color = hover_color
        self.text_color = text_color
        self.corner_radius = corner_radius
        self.is_disabled = False
        
        self._draw_button(self.bg_color)
        
        self.bind("<Enter>", self._on_enter)
        self.bind("<Leave>", self._on_leave)
        self.bind("<Button-1>", self._on_click)
        self.bind("<ButtonRelease-1>", self._on_release)
    
    def _draw_button(self, color: str) -> None:
        self.delete("all")
        r = self.corner_radius
        w, h = self.width, self.height
        
        # Рисуем скругленный прямоугольник
        self.create_arc(0, 0, r*2, r*2, start=90, extent=90, fill=color, outline=color)
        self.create_arc(w-r*2, 0, w, r*2, start=0, extent=90, fill=color, outline=color)
        self.create_arc(0, h-r*2, r*2, h, start=180, extent=90, fill=color, outline=color)
        self.create_arc(w-r*2, h-r*2, w, h, start=270, extent=90, fill=color, outline=color)
        self.create_rectangle(r, 0, w-r, h, fill=color, outline=color)
        self.create_rectangle(0, r, w, h-r, fill=color, outline=color)
        
        # Текст
        self.create_text(
            w/2, h/2,
            text=self.text,
            fill=self.text_color if not self.is_disabled else Colors.TEXT_MUTED,
            font=Fonts.BUTTON
        )
    
    def _on_enter(self, event) -> None:
        if not self.is_disabled:
            self._draw_button(self.hover_color)
    
    def _on_leave(self, event) -> None:
        if not self.is_disabled:
            self._draw_button(self.bg_color)
    
    def _on_click(self, event) -> None:
        if not self.is_disabled:
            self._draw_button(Colors.PRESSED)
    
    def _on_release(self, event) -> None:
        if not self.is_disabled:
            self._draw_button(self.hover_color)
            if self.command:
                self.command()
    
    def config(self, **kwargs) -> None:
        if "state" in kwargs:
            self.is_disabled = kwargs["state"] == "disabled"
            self._draw_button(Colors.BG_LIGHT if self.is_disabled else self.bg_color)
        if "text" in kwargs:
            self.text = kwargs["text"]
            self._draw_button(self.bg_color)


class ModernEntry(tk.Frame):
    """Современное поле ввода с подсветкой фокуса"""
    
    def __init__(
        self,
        master,
        placeholder: str = "",
        show: str = "",
        width: int = 300,
        **kwargs
    ):
        super().__init__(master, bg=Colors.BG_DARK)
        
        self.placeholder = placeholder
        self.show_char = show
        self.is_focused = False
        
        # Контейнер с границей
        self.border_frame = tk.Frame(
            self,
            bg=Colors.BORDER,
            padx=2,
            pady=2
        )
        self.border_frame.pack(fill="x")
        
        # Внутренний контейнер
        self.inner_frame = tk.Frame(
            self.border_frame,
            bg=Colors.BG_LIGHT,
            padx=12,
            pady=10
        )
        self.inner_frame.pack(fill="x")
        
        # Поле ввода
        self.entry = tk.Entry(
            self.inner_frame,
            font=Fonts.INPUT,
            bg=Colors.BG_LIGHT,
            fg=Colors.TEXT_PRIMARY,
            insertbackground=Colors.TEXT_PRIMARY,
            relief="flat",
            width=width // 10,
            show=show
        )
        self.entry.pack(fill="x")
        
        # Placeholder
        if placeholder:
            self.entry.insert(0, placeholder)
            self.entry.config(fg=Colors.TEXT_MUTED)
        
        # Биндинги
        self.entry.bind("<FocusIn>", self._on_focus_in)
        self.entry.bind("<FocusOut>", self._on_focus_out)
    
    def _on_focus_in(self, event) -> None:
        self.is_focused = True
        self.border_frame.config(bg=Colors.PRIMARY)
        if self.entry.get() == self.placeholder:
            self.entry.delete(0, tk.END)
            self.entry.config(fg=Colors.TEXT_PRIMARY)
    
    def _on_focus_out(self, event) -> None:
        self.is_focused = False
        self.border_frame.config(bg=Colors.BORDER)
        if not self.entry.get() and self.placeholder:
            self.entry.insert(0, self.placeholder)
            self.entry.config(fg=Colors.TEXT_MUTED)
    
    def get(self) -> str:
        value = self.entry.get()
        return "" if value == self.placeholder else value
    
    def delete(self, first, last) -> None:
        self.entry.delete(first, last)
    
    def focus(self) -> None:
        self.entry.focus()
    
    def bind(self, sequence, func) -> None:
        self.entry.bind(sequence, func)


class ModernCard(tk.Frame):
    """Карточка с тенью и скруглёнными углами"""
    
    def __init__(self, master, title: str = "", **kwargs):
        super().__init__(master, bg=Colors.BG_CARD, **kwargs)
        
        self.config(padx=24, pady=20)
        
        if title:
            title_label = tk.Label(
                self,
                text=title,
                font=Fonts.SUBTITLE,
                bg=Colors.BG_CARD,
                fg=Colors.TEXT_PRIMARY
            )
            title_label.pack(anchor="w", pady=(0, 16))


class StatusBadge(tk.Frame):
    """Бейдж статуса"""
    
    def __init__(self, master, text: str = "", status: str = "info", **kwargs):
        super().__init__(master, **kwargs)
        
        colors = {
            "success": (Colors.SUCCESS, "#e8f5e9"),
            "warning": (Colors.WARNING, "#fff3e0"),
            "error": (Colors.ERROR, "#ffebee"),
            "info": (Colors.PRIMARY, "#e3f2fd")
        }
        
        bg_color, _ = colors.get(status, colors["info"])
        
        self.config(bg=bg_color, padx=12, pady=4)
        
        self.label = tk.Label(
            self,
            text=text,
            font=Fonts.SMALL,
            bg=bg_color,
            fg=Colors.TEXT_PRIMARY
        )
        self.label.pack()
    
    def set_text(self, text: str) -> None:
        self.label.config(text=text)


# ══════════════════════════════════════════════════════════════════════════════
# ЭКРАН ВХОДА
# ══════════════════════════════════════════════════════════════════════════════

class LoginFrame(tk.Frame):
    def __init__(self, master: tk.Tk, api: ScanpakApi, on_success) -> None:
        super().__init__(master, bg=Colors.BG_DARK)
        self.api = api
        self.on_success = on_success
        self._build_ui()

    def _build_ui(self) -> None:
        # Центральный контейнер
        center_container = tk.Frame(self, bg=Colors.BG_DARK)
        center_container.place(relx=0.5, rely=0.5, anchor="center")
        
        # Логотип / иконка
        logo_frame = tk.Frame(center_container, bg=Colors.BG_DARK)
        logo_frame.pack(pady=(0, 20))
        
        # Круглый логотип
        logo_canvas = tk.Canvas(
            logo_frame,
            width=80,
            height=80,
            bg=Colors.BG_DARK,
            highlightthickness=0
        )
        logo_canvas.pack()
        logo_canvas.create_oval(5, 5, 75, 75, fill=Colors.PRIMARY, outline="")
        logo_canvas.create_text(
            40, 40,
            text="SP",
            font=("Segoe UI", 24, "bold"),
            fill=Colors.TEXT_PRIMARY
        )
        
        # Заголовок
        title_label = tk.Label(
            center_container,
            text="ScanPak",
            font=Fonts.TITLE_LARGE,
            bg=Colors.BG_DARK,
            fg=Colors.TEXT_PRIMARY
        )
        title_label.pack(pady=(0, 5))
        
        subtitle_label = tk.Label(
            center_container,
            text="Система управління скануванням",
            font=Fonts.BODY,
            bg=Colors.BG_DARK,
            fg=Colors.TEXT_SECONDARY
        )
        subtitle_label.pack(pady=(0, 30))
        
        # Карточка формы
        form_card = tk.Frame(
            center_container,
            bg=Colors.BG_CARD,
            padx=40,
            pady=35
        )
        form_card.pack()
        
        # Поля ввода
        surname_label = tk.Label(
            form_card,
            text="Прізвище",
            font=Fonts.BODY_BOLD,
            bg=Colors.BG_CARD,
            fg=Colors.TEXT_SECONDARY
        )
        surname_label.pack(anchor="w", pady=(0, 8))
        
        self.surname_entry = ModernEntry(form_card, placeholder="Введіть прізвище")
        self.surname_entry.pack(fill="x", pady=(0, 20))
        self.surname_entry.inner_frame.config(bg=Colors.BG_LIGHT)
        self.surname_entry.entry.config(bg=Colors.BG_LIGHT)
        
        password_label = tk.Label(
            form_card,
            text="Пароль",
            font=Fonts.BODY_BOLD,
            bg=Colors.BG_CARD,
            fg=Colors.TEXT_SECONDARY
        )
        password_label.pack(anchor="w", pady=(0, 8))
        
        self.password_entry = ModernEntry(form_card, placeholder="Введіть пароль", show="●")
        self.password_entry.pack(fill="x", pady=(0, 10))
        self.password_entry.inner_frame.config(bg=Colors.BG_LIGHT)
        self.password_entry.entry.config(bg=Colors.BG_LIGHT)
        
        # Статус ошибки
        self.status_label = tk.Label(
            form_card,
            text="",
            font=Fonts.SMALL,
            bg=Colors.BG_CARD,
            fg=Colors.ERROR
        )
        self.status_label.pack(anchor="w", pady=(5, 15))
        
        # Кнопка входа
        self.login_button = ModernButton(
            form_card,
            text="Увійти",
            command=self._handle_login,
            width=280,
            height=48,
            bg_color=Colors.PRIMARY,
            hover_color=Colors.PRIMARY_DARK
        )
        self.login_button.pack(pady=(10, 0))
        
        # Версия
        version_label = tk.Label(
            center_container,
            text="v2.0.0 • © 2024 ScanPak Systems",
            font=Fonts.SMALL,
            bg=Colors.BG_DARK,
            fg=Colors.TEXT_MUTED
        )
        version_label.pack(pady=(25, 0))
        
        # Биндинги
        self.surname_entry.focus()
        self.password_entry.bind("<Return>", lambda _: self._handle_login())

    def _handle_login(self) -> None:
        surname = self.surname_entry.get().strip()
        password = self.password_entry.get().strip()

        if not surname or not password:
            self.status_label.config(text="⚠ Введіть прізвище та пароль")
            return

        self.login_button.config(state="disabled")
        self.status_label.config(text="")
        self.update_idletasks()

        try:
            data = self.api.login(surname, password)
        except (requests.RequestException, RuntimeError) as exc:
            self.status_label.config(text=f"⚠ {str(exc)}")
            self.login_button.config(state="normal")
            return

        self.login_button.config(state="normal")
        self.on_success(
            token=str(data.get("token")),
            surname=str(data.get("surname") or surname),
            role=str(data.get("role") or ""),
        )


# ══════════════════════════════════════════════════════════════════════════════
# ГЛАВНЫЙ ЭКРАН
# ══════════════════════════════════════════════════════════════════════════════

class MainFrame(tk.Frame):
    def __init__(self, master: tk.Tk, api: ScanpakApi, session: dict, on_logout) -> None:
        super().__init__(master, bg=Colors.BG_DARK)
        self.api = api
        self.session = session
        self.on_logout = on_logout
        self.records: List[ScanRecord] = []
        self.filtered: List[ScanRecord] = []
        self.current_tab = "scan"
        self._build_ui()
        self._refresh_history()

    def _build_ui(self) -> None:
        # ═══════════════════════════════════════════════════════════════════
        # БОКОВАЯ ПАНЕЛЬ
        # ═══════════════════════════════════════════════════════════════════
        sidebar = tk.Frame(self, bg=Colors.BG_MEDIUM, width=240)
        sidebar.pack(side="left", fill="y")
        sidebar.pack_propagate(False)
        
        # Логотип в сайдбаре
        logo_frame = tk.Frame(sidebar, bg=Colors.BG_MEDIUM)
        logo_frame.pack(fill="x", pady=25, padx=20)
        
        logo_canvas = tk.Canvas(
            logo_frame,
            width=50,
            height=50,
            bg=Colors.BG_MEDIUM,
            highlightthickness=0
        )
        logo_canvas.pack(side="left")
        logo_canvas.create_oval(3, 3, 47, 47, fill=Colors.PRIMARY, outline="")
        logo_canvas.create_text(25, 25, text="SP", font=("Segoe UI", 14, "bold"), fill="white")
        
        logo_text = tk.Label(
            logo_frame,
            text="ScanPak",
            font=Fonts.SUBTITLE,
            bg=Colors.BG_MEDIUM,
            fg=Colors.TEXT_PRIMARY
        )
        logo_text.pack(side="left", padx=12)
        
        # Разделитель
        tk.Frame(sidebar, bg=Colors.BORDER, height=1).pack(fill="x", pady=10)
        
        # Информация о пользователе
        user_frame = tk.Frame(sidebar, bg=Colors.BG_MEDIUM)
        user_frame.pack(fill="x", padx=20, pady=15)
        
        user_icon = tk.Canvas(
            user_frame,
            width=40,
            height=40,
            bg=Colors.BG_MEDIUM,
            highlightthickness=0
        )
        user_icon.pack(side="left")
        user_icon.create_oval(2, 2, 38, 38, fill=Colors.SUCCESS, outline="")
        user_icon.create_text(
            20, 20,
            text=self.session.get('surname', 'U')[0].upper(),
            font=("Segoe UI", 14, "bold"),
            fill="white"
        )
        
        user_info = tk.Frame(user_frame, bg=Colors.BG_MEDIUM)
        user_info.pack(side="left", padx=10)
        
        tk.Label(
            user_info,
            text=self.session.get('surname', 'Користувач'),
            font=Fonts.BODY_BOLD,
            bg=Colors.BG_MEDIUM,
            fg=Colors.TEXT_PRIMARY
        ).pack(anchor="w")
        
        tk.Label(
            user_info,
            text=self.session.get('role', 'Оператор'),
            font=Fonts.SMALL,
            bg=Colors.BG_MEDIUM,
            fg=Colors.TEXT_MUTED
        ).pack(anchor="w")
        
        # Разделитель
        tk.Frame(sidebar, bg=Colors.BORDER, height=1).pack(fill="x", pady=10)
        
        # Навигация
        nav_frame = tk.Frame(sidebar, bg=Colors.BG_MEDIUM)
        nav_frame.pack(fill="x", pady=10)
        
        self.nav_buttons = {}
        
        self.nav_buttons["scan"] = self._create_nav_button(
            nav_frame, "📦  Сканування", "scan", True
        )
        self.nav_buttons["history"] = self._create_nav_button(
            nav_frame, "📋  Історія", "history", False
        )
        
        # Статус в сайдбаре
        self.sidebar_status = tk.Label(
            sidebar,
            text="",
            font=Fonts.SMALL,
            bg=Colors.BG_MEDIUM,
            fg=Colors.SUCCESS,
            wraplength=200
        )
        self.sidebar_status.pack(fill="x", padx=20, pady=20, side="bottom")
        
        # Кнопка выхода
        logout_frame = tk.Frame(sidebar, bg=Colors.BG_MEDIUM)
        logout_frame.pack(fill="x", side="bottom", pady=20, padx=20)
        
        logout_btn = ModernButton(
            logout_frame,
            text="Вийти",
            command=self._logout,
            width=200,
            height=40,
            bg_color=Colors.ERROR,
            hover_color=Colors.ERROR_DARK
        )
        logout_btn.pack()
        
        # ═══════════════════════════════════════════════════════════════════
        # ОСНОВНОЙ КОНТЕНТ
        # ═══════════════════════════════════════════════════════════════════
        self.content_frame = tk.Frame(self, bg=Colors.BG_DARK)
        self.content_frame.pack(side="right", fill="both", expand=True)
        
        # Хедер контента
        header = tk.Frame(self.content_frame, bg=Colors.BG_DARK)
        header.pack(fill="x", padx=30, pady=25)
        
        self.page_title = tk.Label(
            header,
            text="Сканування",
            font=Fonts.TITLE,
            bg=Colors.BG_DARK,
            fg=Colors.TEXT_PRIMARY
        )
        self.page_title.pack(side="left")
        
        # Время и дата
        time_frame = tk.Frame(header, bg=Colors.BG_DARK)
        time_frame.pack(side="right")
        
        self.time_label = tk.Label(
            time_frame,
            text="",
            font=Fonts.BODY,
            bg=Colors.BG_DARK,
            fg=Colors.TEXT_SECONDARY
        )
        self.time_label.pack()
        self._update_time()
        
        # Контейнер для страниц
        self.pages_container = tk.Frame(self.content_frame, bg=Colors.BG_DARK)
        self.pages_container.pack(fill="both", expand=True, padx=30, pady=(0, 30))
        
        # Создаём страницы
        self.scan_page = self._build_scan_page()
        self.history_page = self._build_history_page()
        
        # Показываем страницу сканирования по умолчанию
        self.scan_page.pack(fill="both", expand=True)
    
    def _create_nav_button(self, parent, text: str, tab_name: str, active: bool) -> tk.Frame:
        """Создание кнопки навигации"""
        btn_frame = tk.Frame(parent, bg=Colors.BG_MEDIUM)
        btn_frame.pack(fill="x", pady=2)
        
        bg_color = Colors.PRIMARY if active else Colors.BG_MEDIUM
        
        btn = tk.Label(
            btn_frame,
            text=text,
            font=Fonts.BODY_BOLD if active else Fonts.BODY,
            bg=bg_color,
            fg=Colors.TEXT_PRIMARY,
            padx=20,
            pady=12,
            anchor="w",
            cursor="hand2"
        )
        btn.pack(fill="x", padx=10)
        
        # Индикатор активной вкладки
        if active:
            indicator = tk.Frame(btn_frame, bg=Colors.PRIMARY_LIGHT, width=4)
            indicator.place(x=0, y=0, relheight=1)
        
        btn.bind("<Button-1>", lambda e: self._switch_tab(tab_name))
        btn.bind("<Enter>", lambda e: btn.config(bg=Colors.PRIMARY if active else Colors.HOVER))
        btn.bind("<Leave>", lambda e: btn.config(bg=bg_color))
        
        btn_frame.btn = btn
        btn_frame.active = active
        
        return btn_frame
    
    def _switch_tab(self, tab_name: str) -> None:
        """Переключение вкладки"""
        if tab_name == self.current_tab:
            return
        
        self.current_tab = tab_name
        
        # Обновляем навигацию
        for name, btn_frame in self.nav_buttons.items():
            is_active = name == tab_name
            btn_frame.active = is_active
            btn_frame.btn.config(
                bg=Colors.PRIMARY if is_active else Colors.BG_MEDIUM,
                font=Fonts.BODY_BOLD if is_active else Fonts.BODY
            )
        
        # Переключаем страницы
        self.scan_page.pack_forget()
        self.history_page.pack_forget()
        
        if tab_name == "scan":
            self.page_title.config(text="Сканування")
            self.scan_page.pack(fill="both", expand=True)
        else:
            self.page_title.config(text="Історія сканувань")
            self.history_page.pack(fill="both", expand=True)
    
    def _update_time(self) -> None:
        """Обновление времени"""
        now = dt.datetime.now()
        self.time_label.config(text=now.strftime("%d.%m.%Y  •  %H:%M:%S"))
        self.after(1000, self._update_time)
    
    def _build_scan_page(self) -> tk.Frame:
        """Построение страницы сканирования"""
        page = tk.Frame(self.pages_container, bg=Colors.BG_DARK)
        
        # Основная карточка сканирования
        scan_card = tk.Frame(page, bg=Colors.BG_CARD)
        scan_card.pack(fill="x", pady=(0, 20))
        
        card_content = tk.Frame(scan_card, bg=Colors.BG_CARD, padx=30, pady=30)
        card_content.pack(fill="x")
        
        # Иконка сканера
        icon_frame = tk.Frame(card_content, bg=Colors.BG_CARD)
        icon_frame.pack(pady=(0, 20))
        
        icon_canvas = tk.Canvas(
            icon_frame,
            width=70,
            height=70,
            bg=Colors.BG_CARD,
            highlightthickness=0
        )
        icon_canvas.pack()
        icon_canvas.create_oval(5, 5, 65, 65, fill=Colors.PRIMARY, outline="")
        icon_canvas.create_text(35, 35, text="📦", font=("Segoe UI", 24))
        
        # Заголовок
        tk.Label(
            card_content,
            text="Сканування посилки",
            font=Fonts.SUBTITLE,
            bg=Colors.BG_CARD,
            fg=Colors.TEXT_PRIMARY
        ).pack(pady=(0, 5))
        
        tk.Label(
            card_content,
            text="Введіть або відскануйте Box ID",
            font=Fonts.BODY,
            bg=Colors.BG_CARD,
            fg=Colors.TEXT_SECONDARY
        ).pack(pady=(0, 25))
        
        # Поле ввода
        input_frame = tk.Frame(card_content, bg=Colors.BG_CARD)
        input_frame.pack(fill="x")
        
        self.scan_entry = ModernEntry(input_frame, placeholder="Box ID")
        self.scan_entry.pack(pady=(0, 20))
        self.scan_entry.inner_frame.config(bg=Colors.BG_LIGHT)
        self.scan_entry.entry.config(bg=Colors.BG_LIGHT, width=40)
        self.scan_entry.bind("<Return>", lambda _: self._handle_scan())
        
        # Кнопка отправки
        send_btn = ModernButton(
            card_content,
            text="Надіслати",
            command=self._handle_scan,
            width=200,
            height=48,
            bg_color=Colors.SUCCESS,
            hover_color=Colors.SUCCESS_DARK
        )
        send_btn.pack(pady=(0, 20))
        
        # Фидбек
        self.scan_feedback = tk.Label(
            card_content,
            text="",
            font=Fonts.BODY,
            bg=Colors.BG_CARD,
            fg=Colors.SUCCESS
        )
        self.scan_feedback.pack()
        
        # Статистика
        stats_frame = tk.Frame(page, bg=Colors.BG_DARK)
        stats_frame.pack(fill="x")
        
        self._create_stat_card(stats_frame, "Сьогодні", "0", Colors.PRIMARY).pack(side="left", fill="x", expand=True, padx=(0, 10))
        self._create_stat_card(stats_frame, "Цього тижня", "0", Colors.SUCCESS).pack(side="left", fill="x", expand=True, padx=10)
        self._create_stat_card(stats_frame, "Всього", "0", Colors.WARNING).pack(side="left", fill="x", expand=True, padx=(10, 0))
        
        return page
    
    def _create_stat_card(self, parent, title: str, value: str, color: str) -> tk.Frame:
        """Создание карточки статистики"""
        card = tk.Frame(parent, bg=Colors.BG_CARD)
        
        content = tk.Frame(card, bg=Colors.BG_CARD, padx=20, pady=20)
        content.pack(fill="both", expand=True)
        
        tk.Label(
            content,
            text=title,
            font=Fonts.SMALL,
            bg=Colors.BG_CARD,
            fg=Colors.TEXT_MUTED
        ).pack(anchor="w")
        
        tk.Label(
            content,
            text=value,
            font=Fonts.TITLE,
            bg=Colors.BG_CARD,
            fg=color
        ).pack(anchor="w", pady=(5, 0))
        
        return card
    
    def _build_history_page(self) -> tk.Frame:
        """Построение страницы истории"""
        page = tk.Frame(self.pages_container, bg=Colors.BG_DARK)
        
        # Фильтры
        filters_card = tk.Frame(page, bg=Colors.BG_CARD)
        filters_card.pack(fill="x", pady=(0, 20))
        
        filters_content = tk.Frame(filters_card, bg=Colors.BG_CARD, padx=25, pady=20)
        filters_content.pack(fill="x")
        
        tk.Label(
            filters_content,
            text="🔍 Фільтри",
            font=Fonts.SUBTITLE,
            bg=Colors.BG_CARD,
            fg=Colors.TEXT_PRIMARY
        ).pack(anchor="w", pady=(0, 15))
        
        # Строка фильтров
        filter_row = tk.Frame(filters_content, bg=Colors.BG_CARD)
        filter_row.pack(fill="x")
        
        # Box ID фильтр
        box_filter_frame = tk.Frame(filter_row, bg=Colors.BG_CARD)
        box_filter_frame.pack(side="left", fill="x", expand=True, padx=(0, 10))
        
        tk.Label(
            box_filter_frame,
            text="Box ID",
            font=Fonts.SMALL,
            bg=Colors.BG_CARD,
            fg=Colors.TEXT_SECONDARY
        ).pack(anchor="w", pady=(0, 5))
        
        self.filter_box_entry = ModernEntry(box_filter_frame, placeholder="")
        self.filter_box_entry.pack(fill="x")
        self.filter_box_entry.inner_frame.config(bg=Colors.BG_LIGHT)
        self.filter_box_entry.entry.config(bg=Colors.BG_LIGHT, width=15)
        
        # Користувач фильтр
        user_filter_frame = tk.Frame(filter_row, bg=Colors.BG_CARD)
        user_filter_frame.pack(side="left", fill="x", expand=True, padx=10)
        
        tk.Label(
            user_filter_frame,
            text="Користувач",
            font=Fonts.SMALL,
            bg=Colors.BG_CARD,
            fg=Colors.TEXT_SECONDARY
        ).pack(anchor="w", pady=(0, 5))
        
        self.filter_user_entry = ModernEntry(user_filter_frame, placeholder="")
        self.filter_user_entry.pack(fill="x")
        self.filter_user_entry.inner_frame.config(bg=Colors.BG_LIGHT)
        self.filter_user_entry.entry.config(bg=Colors.BG_LIGHT, width=15)
        
        # Дата фильтр
        date_filter_frame = tk.Frame(filter_row, bg=Colors.BG_CARD)
        date_filter_frame.pack(side="left", fill="x", expand=True, padx=(10, 0))
        
        tk.Label(
            date_filter_frame,
            text="Дата (YYYY-MM-DD)",
            font=Fonts.SMALL,
            bg=Colors.BG_CARD,
            fg=Colors.TEXT_SECONDARY
        ).pack(anchor="w", pady=(0, 5))
        
        self.filter_date_entry = ModernEntry(date_filter_frame, placeholder="")
        self.filter_date_entry.pack(fill="x")
        self.filter_date_entry.inner_frame.config(bg=Colors.BG_LIGHT)
        self.filter_date_entry.entry.config(bg=Colors.BG_LIGHT, width=15)
        
        # Кнопки фильтров
        buttons_row = tk.Frame(filters_content, bg=Colors.BG_CARD)
        buttons_row.pack(fill="x", pady=(15, 0))
        
        apply_btn = ModernButton(
            buttons_row,
            text="Застосувати",
            command=self._apply_filters,
            width=130,
            height=38,
            bg_color=Colors.PRIMARY,
            hover_color=Colors.PRIMARY_DARK
        )
        apply_btn.pack(side="left", padx=(0, 10))
        
        clear_btn = ModernButton(
            buttons_row,
            text="Очистити",
            command=self._clear_filters,
            width=110,
            height=38,
            bg_color=Colors.BG_LIGHT,
            hover_color=Colors.HOVER
        )
        clear_btn.pack(side="left", padx=(0, 10))
        
        refresh_btn = ModernButton(
            buttons_row,
            text="Оновити",
            command=self._refresh_history,
            width=110,
            height=38,
            bg_color=Colors.SUCCESS,
            hover_color=Colors.SUCCESS_DARK
        )
        refresh_btn.pack(side="left")
        
        # Таблица
        table_card = tk.Frame(page, bg=Colors.BG_CARD)
        table_card.pack(fill="both", expand=True)
        
        table_content = tk.Frame(table_card, bg=Colors.BG_CARD, padx=20, pady=20)
        table_content.pack(fill="both", expand=True)
        
        # Стилизация Treeview
        style = ttk.Style()
        style.theme_use("clam")
        
        style.configure(
            "Custom.Treeview",
            background=Colors.BG_LIGHT,
            foreground=Colors.TEXT_PRIMARY,
            fieldbackground=Colors.BG_LIGHT,
            borderwidth=0,
            font=Fonts.BODY,
            rowheight=40
        )
        
        style.configure(
            "Custom.Treeview.Heading",
            background=Colors.BG_MEDIUM,
            foreground=Colors.TEXT_PRIMARY,
            borderwidth=0,
            font=Fonts.BODY_BOLD
        )
        
        style.map(
            "Custom.Treeview",
            background=[("selected", Colors.PRIMARY)],
            foreground=[("selected", Colors.TEXT_PRIMARY)]
        )
        
        # Контейнер для таблицы со скроллбаром
        tree_frame = tk.Frame(table_content, bg=Colors.BG_CARD)
        tree_frame.pack(fill="both", expand=True)
        
        self.history_tree = ttk.Treeview(
            tree_frame,
            columns=("number", "user", "time"),
            show="headings",
            style="Custom.Treeview"
        )
        
        self.history_tree.heading("number", text="Box ID")
        self.history_tree.heading("user", text="Користувач")
        self.history_tree.heading("time", text="Час сканування")
        
        self.history_tree.column("number", width=180, anchor="center")
        self.history_tree.column("user", width=180, anchor="center")
        self.history_tree.column("time", width=220, anchor="center")
        
        # Скроллбар
        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.history_tree.yview)
        self.history_tree.configure(yscrollcommand=scrollbar.set)
        
        self.history_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        return page
    
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
        self.sidebar_status.config(text="⏳ Оновлюємо історію...", fg=Colors.WARNING)
        self.update_idletasks()
        try:
            self.records = self.api.fetch_history(token)
            self.filtered = list(self.records)
            self._render_history()
            self._update_stats()
            self.sidebar_status.config(text="✓ Історія оновлена", fg=Colors.SUCCESS)
        except (requests.RequestException, RuntimeError) as exc:
            self.sidebar_status.config(text=f"⚠ {str(exc)}", fg=Colors.ERROR)

    def _render_history(self) -> None:
        self.history_tree.delete(*self.history_tree.get_children())
        for record in self.filtered:
            display_time = record.timestamp.strftime("%Y-%m-%d  •  %H:%M:%S")
            self.history_tree.insert(
                "", tk.END, values=(record.number, record.user, display_time)
            )
    
    def _update_stats(self) -> None:
        """Обновление статистики"""
        today = dt.date.today()
        week_start = today - dt.timedelta(days=today.weekday())
        
        today_count = sum(1 for r in self.records if r.timestamp.date() == today)
        week_count = sum(1 for r in self.records if r.timestamp.date() >= week_start)
        total_count = len(self.records)
        
        # Обновляем карточки статистики (если они есть)
        # В данной реализации статистика статична, но её можно сделать динамической

    def _handle_scan(self) -> None:
        raw = self.scan_entry.get().strip()
        digits = "".join(ch for ch in raw if ch.isdigit())
        if not digits:
            self.scan_feedback.config(text="⚠ Не знайшли цифр у введенні", fg=Colors.WARNING)
            self.scan_entry.focus()
            return

        if self._is_duplicate(digits):
            self.scan_feedback.config(text="⚠ Увага, це дублікат. Не збережено", fg=Colors.WARNING)
            self.scan_entry.delete(0, tk.END)
            self.scan_entry.focus()
            return

        token = self.session.get("token")
        if not token:
            messagebox.showwarning("Сесія", "Сесію завершено. Увійдіть знову")
            self.on_logout()
            return

        self.scan_feedback.config(text="⏳ Відправляємо...", fg=Colors.TEXT_SECONDARY)
        self.update_idletasks()
        try:
            record = self.api.send_scan(token, digits)
        except (requests.RequestException, RuntimeError) as exc:
            messagebox.showerror("Сканування", str(exc))
            self.scan_feedback.config(text="⚠ Помилка збереження", fg=Colors.ERROR)
            return

        self.records.insert(0, record)
        self.filtered = list(self.records)
        self._render_history()
        self.scan_feedback.config(
            text=f"✓ Збережено для {record.user} о {record.timestamp.strftime('%H:%M')}",
            fg=Colors.SUCCESS
        )
        self.scan_entry.delete(0, tk.END)
        self.scan_entry.focus()

    def _is_duplicate(self, digits: str) -> bool:
        return any(record.number == digits for record in self.records)


# ══════════════════════════════════════════════════════════════════════════════
# ГЛАВНОЕ ПРИЛОЖЕНИЕ
# ══════════════════════════════════════════════════════════════════════════════

class ScanpakApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("ScanPak — Система управління скануванням")
        self.geometry("1100x700")
        self.minsize(900, 600)
        self.configure(bg=Colors.BG_DARK)
        
        # Иконка приложения (опционально)
        # self.iconbitmap("icon.ico")
        
        self.api = ScanpakApi(API_HOST, API_PORT, API_BASE_PATH)
        self.session: dict = {}

        self.container = tk.Frame(self, bg=Colors.BG_DARK)
        self.container.pack(fill="both", expand=True)

        self.login_frame: Optional[LoginFrame] = LoginFrame(
            self.container, self.api, self._handle_login
        )
        self.main_frame: Optional[MainFrame] = None
        self.login_frame.pack(fill="both", expand=True)
        
        # Центрирование окна
        self._center_window()
    
    def _center_window(self) -> None:
        """Центрирование окна на экране"""
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f"{width}x{height}+{x}+{y}")

    def _handle_login(self, token: str, surname: str, role: str) -> None:
        self.session = {"token": token, "surname": surname, "role": role}
        if self.login_frame:
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
