import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

# ─── КОНФИГУРАЦИЯ И КОНСТАНТЫ ──────────────────────────────────────
DB_FILE = "tsd_registry.db"
APP_TITLE = "TSD Enterprise | Учет оборудования"
APP_SIZE = "1280x800"

# Современная корпоративная палитра (Slate & Blue)
COLORS = {
    "bg_app":          "#F3F4F6",  # Очень светло-серый фон
    "bg_sidebar":      "#111827",  # Почти черный (Deep Navy)
    "bg_card":         "#FFFFFF",  # Чистый белый
    "primary":         "#2563EB",  # Яркий корпоративный синий
    "primary_hover":   "#1D4ED8",  # Темнее при наведении
    "secondary":       "#64748B",  # Серый для второстепенного текста
    "text_main":       "#1F2937",  # Темно-серый для основного текста
    "text_light":      "#9CA3AF",  # Светло-серый
    "text_on_dark":    "#F9FAFB",  # Белый текст на темном фоне
    "border":          "#E5E7EB",  # Светлая граница
    "success":         "#10B981",  # Зеленый
    "warning":         "#F59E0B",  # Оранжевый
    "danger":          "#EF4444",  # Красный
    "row_stripe":      "#F9FAFB",  # Цвет чередования строк
    "row_hover":       "#EFF6FF",  # Подсветка строки (светло-голубой)
}

FONTS = {
    "h1": ("Segoe UI", 24, "bold"),
    "h2": ("Segoe UI", 16, "bold"),
    "h3": ("Segoe UI", 12, "bold"),
    "body": ("Segoe UI", 10),
    "body_bold": ("Segoe UI", 10, "bold"),
    "small": ("Segoe UI", 9),
    "icon": ("Segoe UI Emoji", 14),  # Для эмодзи-иконок
}

# ─── КЛАСС ПРИЛОЖЕНИЯ ──────────────────────────────────────────────
class TSDRegistryApp:
    def __init__(self, root):
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry(APP_SIZE)
        self.root.minsize(1000, 600)
        self.root.configure(bg=COLORS["bg_app"])

        self.is_fullscreen = False
        self.current_page = None
        
        # Подключение к БД
        self.conn = sqlite3.connect(DB_FILE)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

        # Инициализация стилей и интерфейса
        self._setup_styles()
        self._build_layout()
        
        # Загрузка первой страницы
        self.show_page("registry")
        self.refresh_all_data()

        # Бинды
        self.root.bind("<F11>", self._toggle_fullscreen)
        self.root.bind("<Escape>", self._exit_fullscreen)

    def _init_db(self):
        """Инициализация таблиц базы данных."""
        with self.conn:
            cur = self.conn.cursor()
            cur.execute("""CREATE TABLE IF NOT EXISTS statuses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )""")
            cur.execute("""CREATE TABLE IF NOT EXISTS devices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                brand TEXT NOT NULL,
                model TEXT NOT NULL,
                imei TEXT UNIQUE NOT NULL,
                status_id INTEGER,
                employee TEXT DEFAULT 'Свободный',
                location_id INTEGER,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(status_id) REFERENCES statuses(id),
                FOREIGN KEY(location_id) REFERENCES locations(id)
            )""")

    # ─── ДИЗАЙН И СТИЛИ ────────────────────────────────────────────
    def _setup_styles(self):
        style = ttk.Style()
        style.theme_use("clam")  # Основа для кастомизации

        # -- Общие --
        style.configure("TFrame", background=COLORS["bg_app"])
        style.configure("Card.TFrame", background=COLORS["bg_card"], relief="flat")
        
        # -- Метки (Labels) --
        style.configure("TLabel", background=COLORS["bg_app"], foreground=COLORS["text_main"], font=FONTS["body"])
        style.configure("Card.TLabel", background=COLORS["bg_card"], foreground=COLORS["text_main"], font=FONTS["body"])
        style.configure("Header.TLabel", background=COLORS["bg_app"], foreground=COLORS["text_main"], font=FONTS["h1"])
        style.configure("CardHeader.TLabel", background=COLORS["bg_card"], foreground=COLORS["text_main"], font=FONTS["h2"])
        style.configure("SubHeader.TLabel", background=COLORS["bg_app"], foreground=COLORS["secondary"], font=FONTS["body"])
        style.configure("StatValue.TLabel", background=COLORS["bg_card"], foreground=COLORS["primary"], font=("Segoe UI", 32, "bold"))
        style.configure("StatLabel.TLabel", background=COLORS["bg_card"], foreground=COLORS["secondary"], font=FONTS["small"])

        # -- Кнопки (Buttons) --
        # Primary Action Button
        style.configure("Primary.TButton",
                        font=FONTS["body_bold"],
                        background=COLORS["primary"],
                        foreground="white",
                        borderwidth=0,
                        focuscolor=COLORS["primary"],
                        padding=(20, 10))
        style.map("Primary.TButton",
                  background=[("active", COLORS["primary_hover"]), ("disabled", COLORS["secondary"])])

        # Danger Button
        style.configure("Danger.TButton",
                        font=FONTS["body_bold"],
                        background=COLORS["danger"],
                        foreground="white",
                        borderwidth=0,
                        padding=(15, 8))
        style.map("Danger.TButton", background=[("active", "#DC2626")])

        # Ghost/Outline Button
        style.configure("Ghost.TButton",
                        font=FONTS["body"],
                        background=COLORS["bg_app"],
                        foreground=COLORS["text_main"],
                        borderwidth=1,
                        bordercolor=COLORS["border"],
                        padding=(15, 8))
        style.map("Ghost.TButton", background=[("active", "#E5E7EB")])

        # -- Таблицы (Treeview) --
        # Современный вид таблицы: высокие строки, без границ ячеек
        style.configure("Treeview",
                        background=COLORS["bg_card"],
                        fieldbackground=COLORS["bg_card"],
                        foreground=COLORS["text_main"],
                        font=FONTS["body"],
                        rowheight=45,  # Высокие строки для удобства
                        borderwidth=0)
        
        style.configure("Treeview.Heading",
                        background=COLORS["bg_app"],
                        foreground=COLORS["secondary"],
                        font=FONTS["body_bold"],
                        padding=(10, 10),
                        relief="flat")
        
        style.map("Treeview",
                  background=[("selected", COLORS["row_hover"])],
                  foreground=[("selected", COLORS["primary"])])

        # -- Скроллбар --
        style.layout("Vertical.TScrollbar",
                     [('Vertical.Scrollbar.trough',
                       {'children': [('Vertical.Scrollbar.thumb', 
                                      {'expand': '1', 'sticky': 'nswe'})],
                        'sticky': 'ns'})])
        style.configure("Vertical.TScrollbar", troughcolor=COLORS["bg_app"], background="#CBD5E1", borderwidth=0, width=10)

        # -- Поля ввода --
        style.configure("TEntry", fieldbackground=COLORS["bg_card"], borderwidth=1, padding=5)

    # ─── UI LAYOUT ─────────────────────────────────────────────────
    def _build_layout(self):
        # Основной контейнер: Сетка 2 колонки (Сайдбар фикс, Контент растягивается)
        self.main_container = tk.Frame(self.root, bg=COLORS["bg_app"])
        self.main_container.pack(fill="both", expand=True)
        self.main_container.columnconfigure(1, weight=1)
        self.main_container.rowconfigure(0, weight=1)

        self._build_sidebar()
        self._build_content_area()

    def _build_sidebar(self):
        # Сайдбар (левая колонка)
        self.sidebar = tk.Frame(self.main_container, bg=COLORS["bg_sidebar"], width=260)
        self.sidebar.grid(row=0, column=0, sticky="ns")
        self.sidebar.grid_propagate(False)

        # Логотип
        logo_frame = tk.Frame(self.sidebar, bg=COLORS["bg_sidebar"])
        logo_frame.pack(fill="x", pady=(30, 40), padx=25)
        
        # Имитация логотипа
        tk.Label(logo_frame, text="TSD", fg="white", bg=COLORS["primary"], 
                 font=("Segoe UI", 14, "bold"), width=3).pack(side="left")
        tk.Label(logo_frame, text="Enterprise", fg="white", bg=COLORS["bg_sidebar"], 
                 font=("Segoe UI", 16, "bold")).pack(side="left", padx=10)

        # Меню навигации
        self.nav_btns = {}
        self._add_sidebar_btn("registry", "📋  Реестр оборудования")
        self._add_sidebar_btn("catalog", "📁  Справочники")
        self._add_sidebar_btn("stats", "📊  Аналитика")

        # Нижняя кнопка (Полный экран)
        tk.Frame(self.sidebar, bg="#1F2937", height=1).pack(side="bottom", fill="x", pady=0)
        btn = tk.Button(self.sidebar, text="⛶  На весь экран", 
                        bg=COLORS["bg_sidebar"], fg=COLORS["text_light"],
                        font=FONTS["body"], bd=0, activebackground="#1F2937", activeforeground="white",
                        cursor="hand2", command=self._toggle_fullscreen, anchor="w", padx=25, pady=20)
        btn.pack(side="bottom", fill="x")

    def _add_sidebar_btn(self, key, text):
        # Используем tk.Button, так как их проще красить чем ttk
        btn = tk.Button(self.sidebar, text=text, 
                        bg=COLORS["bg_sidebar"], fg=COLORS["text_light"],
                        font=FONTS["body"], bd=0, 
                        activebackground="#1F2937", activeforeground="white",
                        cursor="hand2", anchor="w", padx=25, pady=15,
                        command=lambda k=key: self.show_page(k))
        btn.pack(fill="x", pady=2)
        self.nav_btns[key] = btn

    def _build_content_area(self):
        # Правая часть
        self.content_frame = tk.Frame(self.main_container, bg=COLORS["bg_app"])
        self.content_frame.grid(row=0, column=1, sticky="nsew", padx=30, pady=30)
        
        # Заголовок страницы + Кнопка обновить
        self.top_bar = tk.Frame(self.content_frame, bg=COLORS["bg_app"])
        self.top_bar.pack(fill="x", pady=(0, 20))
        
        self.page_title = ttk.Label(self.top_bar, text="Заголовок", style="Header.TLabel")
        self.page_title.pack(side="left")
        
        self.page_subtitle = ttk.Label(self.top_bar, text="Описание", style="SubHeader.TLabel")
        self.page_subtitle.pack(side="left", padx=(15, 0), pady=(8, 0))

        ttk.Button(self.top_bar, text="🔄 Обновить данные", style="Ghost.TButton", 
                   command=self.refresh_all_data).pack(side="right")

        # Контейнер для сменяемых страниц
        self.pages_container = tk.Frame(self.content_frame, bg=COLORS["bg_app"])
        self.pages_container.pack(fill="both", expand=True)

        self.pages = {}
        for p in ["registry", "catalog", "stats"]:
            frame = tk.Frame(self.pages_container, bg=COLORS["bg_app"])
            self.pages[p] = frame
            # Grid configure для страниц, чтобы контент растягивался
            frame.columnconfigure(0, weight=1)
            frame.rowconfigure(0, weight=1)

        self._init_page_registry()
        self._init_page_catalog()
        self._init_page_stats()

    # ─── ЛОГИКА НАВИГАЦИИ ──────────────────────────────────────────
    def show_page(self, key):
        self.current_page = key
        
        # Обновление меню
        for k, btn in self.nav_btns.items():
            if k == key:
                btn.configure(bg="#1F2937", fg="white", font=FONTS["body_bold"], borderwidth=0)
                # Добавляем синюю полоску слева (имитация border-left)
            else:
                btn.configure(bg=COLORS["bg_sidebar"], fg=COLORS["text_light"], font=FONTS["body"])

        # Обновление заголовков
        titles = {
            "registry": ("Реестр ТСД", "Управление парком терминалов"),
            "catalog": ("Справочники", "Настройка локаций и статусов"),
            "stats": ("Аналитика", "Сводная информация по оборудованию")
        }
        t, s = titles.get(key, ("", ""))
        self.page_title.configure(text=t)
        self.page_subtitle.configure(text=s)

        # Смена кадра
        for frame in self.pages.values():
            frame.pack_forget()
        self.pages[key].pack(fill="both", expand=True)

    # ═══════════════════════════════════════════════════════════════
    #  СТРАНИЦА: РЕЕСТР
    # ═══════════════════════════════════════════════════════════════
    def _init_page_registry(self):
        p = self.pages["registry"]
        
        # Панель инструментов (Поиск + Действия)
        toolbar = tk.Frame(p, bg=COLORS["bg_app"])
        toolbar.pack(fill="x", pady=(0, 15))

        # Поиск
        search_cont = tk.Frame(toolbar, bg="white", highlightbackground=COLORS["border"], highlightthickness=1)
        search_cont.pack(side="left")
        tk.Label(search_cont, text="🔍", bg="white", fg=COLORS["secondary"]).pack(side="left", padx=(10, 5))
        
        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", lambda *a: self._load_registry(self.search_var.get()))
        entry = tk.Entry(search_cont, textvariable=self.search_var, font=FONTS["body"], 
                         bd=0, bg="white", width=30)
        entry.pack(side="left", ipady=8, padx=5)

        ttk.Button(toolbar, text="＋ Добавить ТСД", style="Primary.TButton", 
                   command=self._open_device_dialog).pack(side="right")

        # Карточка с таблицей
        card = tk.Frame(p, bg=COLORS["bg_card"], padx=1, pady=1) # Тонкая рамка за счет паддинга
        card.pack(fill="both", expand=True)
        
        # Сама таблица
        cols = ("id", "brand", "model", "imei", "status", "employee", "location", "updated")
        headers = {"id": "#", "brand": "Бренд", "model": "Модель", "imei": "IMEI", 
                   "status": "Статус", "employee": "Сотрудник", "location": "Локация", "updated": "Обновлено"}
        
        self.tree_reg = ttk.Treeview(card, columns=cols, show="headings", style="Treeview")
        
        for col in cols:
            self.tree_reg.heading(col, text=headers[col], anchor="w")
            self.tree_reg.column(col, anchor="w", width=100)
        
        # Настройка ширины
        self.tree_reg.column("id", width=50, stretch=False)
        self.tree_reg.column("imei", width=150)
        self.tree_reg.column("updated", width=140)

        vsb = ttk.Scrollbar(card, orient="vertical", command=self.tree_reg.yview, style="Vertical.TScrollbar")
        self.tree_reg.configure(yscrollcommand=vsb.set)
        
        self.tree_reg.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree_reg.bind("<Double-1>", self._on_registry_double_click)

    # ═══════════════════════════════════════════════════════════════
    #  СТРАНИЦА: СПРАВОЧНИКИ
    # ═══════════════════════════════════════════════════════════════
    def _init_page_catalog(self):
        p = self.pages["catalog"]
        
        # Сетка 2x2 для карточек справочников
        p.columnconfigure(0, weight=1)
        p.columnconfigure(1, weight=1)
        p.rowconfigure(0, weight=1)
        p.rowconfigure(1, weight=1)

        # 1. Локации
        self._create_catalog_card(p, "Локации", "location", 0, 0)
        # 2. Статусы
        self._create_catalog_card(p, "Статусы устройств", "status", 0, 1)
        # 3. Список всех устройств (Упрощенный)
        self._create_catalog_card(p, "Список устройств (Управление)", "device_simple", 1, 0, colspan=2)

    def _create_catalog_card(self, parent, title, kind, row, col, colspan=1):
        # Обертка карточки
        frame = tk.Frame(parent, bg=COLORS["bg_card"], padx=20, pady=20)
        frame.grid(row=row, column=col, columnspan=colspan, sticky="nsew", padx=(0, 20), pady=(0, 20))
        
        # Хедер карточки
        h_frame = tk.Frame(frame, bg=COLORS["bg_card"])
        h_frame.pack(fill="x", mb=15)
        ttk.Label(h_frame, text=title, style="CardHeader.TLabel").pack(side="left")
        
        # Кнопки действий
        btn_frame = tk.Frame(h_frame, bg=COLORS["bg_card"])
        btn_frame.pack(side="right")
        
        if kind != "device_simple":
            add_cmd = lambda: self._open_dict_dialog(kind)
            edit_cmd = lambda: self._action_dict(kind, "edit")
            del_cmd = lambda: self._action_dict(kind, "delete")
        else:
            add_cmd = self._open_device_dialog
            edit_cmd = self._edit_selected_device_simple
            del_cmd = self._delete_selected_device_simple

        ttk.Button(btn_frame, text="+", style="Ghost.TButton", width=3, command=add_cmd).pack(side="left", padx=2)
        ttk.Button(btn_frame, text="✎", style="Ghost.TButton", width=3, command=edit_cmd).pack(side="left", padx=2)
        ttk.Button(btn_frame, text="✕", style="Danger.TButton", width=3, command=del_cmd).pack(side="left", padx=2)

        # Таблица
        if kind == "device_simple":
            cols = ("id", "brand", "model", "imei")
            headers = {"id": "#", "brand": "Бренд", "model": "Модель", "imei": "IMEI"}
        else:
            cols = ("id", "name")
            headers = {"id": "#", "name": "Название"}

        tree = ttk.Treeview(frame, columns=cols, show="headings", style="Treeview", height=6)
        for c in cols:
            tree.heading(c, text=headers[c], anchor="w")
            tree.column(c, anchor="w", width=100)
        tree.column("id", width=40, stretch=False)
        
        tree.pack(fill="both", expand=True)
        
        # Сохраняем ссылку на дерево
        if kind == "location": self.tree_loc = tree
        elif kind == "status": self.tree_stat = tree
        elif kind == "device_simple": self.tree_dev_s = tree

    # ═══════════════════════════════════════════════════════════════
    #  СТРАНИЦА: АНАЛИТИКА
    # ═══════════════════════════════════════════════════════════════
    def _init_page_stats(self):
        p = self.pages["stats"]
        
        # Верхние виджеты (KPI)
        kpi_frame = tk.Frame(p, bg=COLORS["bg_app"])
        kpi_frame.pack(fill="x", pady=(0, 20))
        
        self.kpi_labels = {}
        for idx, (key, title) in enumerate([("total", "Всего устройств"), ("assigned", "В работе"), ("free", "На складе")]):
            card = tk.Frame(kpi_frame, bg=COLORS["bg_card"], padx=25, pady=20)
            card.pack(side="left", fill="both", expand=True, padx=(0, 20) if idx < 2 else 0)
            
            ttk.Label(card, text=title, style="StatLabel.TLabel").pack(anchor="w")
            lbl = ttk.Label(card, text="0", style="StatValue.TLabel")
            lbl.pack(anchor="w", pady=(5, 0))
            self.kpi_labels[key] = lbl

        # Детальная статистика (Таблица)
        detail_frame = tk.Frame(p, bg=COLORS["bg_card"], padx=25, pady=25)
        detail_frame.pack(fill="both", expand=True)
        
        ttk.Label(detail_frame, text="Детализация по статусам", style="CardHeader.TLabel").pack(anchor="w", mb=15)
        
        cols = ("status", "count", "percent")
        self.tree_stats = ttk.Treeview(detail_frame, columns=cols, show="headings", style="Treeview")
        self.tree_stats.heading("status", text="Статус", anchor="w")
        self.tree_stats.heading("count", text="Количество", anchor="w")
        self.tree_stats.heading("percent", text="Доля %", anchor="w")
        self.tree_stats.pack(fill="both", expand=True)

    # ═══════════════════════════════════════════════════════════════
    #  РАБОТА С ДАННЫМИ
    # ═══════════════════════════════════════════════════════════════
    def refresh_all_data(self):
        self._load_registry()
        self._load_catalogs()
        self._load_stats()

    def _load_registry(self, search_query=""):
        self._clear_tree(self.tree_reg)
        cur = self.conn.cursor()
        sql = """
            SELECT d.id, d.brand, d.model, d.imei, 
                   s.name as status, d.employee, l.name as location, d.updated_at
            FROM devices d
            LEFT JOIN statuses s ON d.status_id = s.id
            LEFT JOIN locations l ON d.location_id = l.id
            WHERE 1=1
        """
        params = []
        if search_query:
            q = f"%{search_query.strip()}%"
            sql += " AND (d.brand LIKE ? OR d.model LIKE ? OR d.imei LIKE ? OR d.employee LIKE ?)"
            params = [q, q, q, q]
        
        sql += " ORDER BY d.updated_at DESC"
        cur.execute(sql, params)
        
        for i, row in enumerate(cur.fetchall()):
            vals = list(row)
            # Простая замена None на строки
            vals = [v if v is not None else "—" for v in vals]
            
            # Чередование цветов
            tag = "even" if i % 2 == 0 else "odd"
            self.tree_reg.insert("", "end", values=vals, tags=(tag,))
        
        # Настройка цветов строк
        self.tree_reg.tag_configure("odd", background=COLORS["row_stripe"])
        self.tree_reg.tag_configure("even", background=COLORS["bg_card"])

    def _load_catalogs(self):
        self._clear_tree(self.tree_loc)
        self._clear_tree(self.tree_stat)
        self._clear_tree(self.tree_dev_s)
        
        cur = self.conn.cursor()
        
        # Локации
        cur.execute("SELECT id, name FROM locations ORDER BY name")
        for r in cur.fetchall(): self.tree_loc.insert("", "end", values=list(r))
        
        # Статусы
        cur.execute("SELECT id, name FROM statuses ORDER BY name")
        for r in cur.fetchall(): self.tree_stat.insert("", "end", values=list(r))
        
        # Устройства (простой вид)
        cur.execute("SELECT id, brand, model, imei FROM devices ORDER BY brand, model")
        for r in cur.fetchall(): self.tree_dev_s.insert("", "end", values=list(r))

    def _load_stats(self):
        cur = self.conn.cursor()
        
        # KPI
        cur.execute("SELECT COUNT(*) as cnt FROM devices")
        total = cur.fetchone()['cnt']
        
        cur.execute("SELECT COUNT(*) as cnt FROM devices WHERE employee IS NOT NULL AND employee != 'Свободный'")
        assigned = cur.fetchone()['cnt']
        
        free = total - assigned
        
        self.kpi_labels["total"].config(text=str(total))
        self.kpi_labels["assigned"].config(text=str(assigned))
        self.kpi_labels["free"].config(text=str(free))

        # Детализация
        self._clear_tree(self.tree_stats)
        cur.execute("""
            SELECT s.name, COUNT(d.id) as cnt 
            FROM devices d 
            JOIN statuses s ON d.status_id = s.id 
            GROUP BY s.id
        """)
        rows = cur.fetchall()
        for r in rows:
            name, cnt = r['name'], r['cnt']
            pct = f"{(cnt/total*100):.1f}%" if total > 0 else "0%"
            self.tree_stats.insert("", "end", values=(name, cnt, pct))

    def _clear_tree(self, tree):
        for item in tree.get_children():
            tree.delete(item)

    # ═══════════════════════════════════════════════════════════════
    #  ДИАЛОГИ И ДЕЙСТВИЯ
    # ═══════════════════════════════════════════════════════════════
    def _create_modal(self, title, width=500, height=400):
        top = tk.Toplevel(self.root)
        top.title(title)
        top.geometry(f"{width}x{height}")
        top.configure(bg=COLORS["bg_card"])
        top.transient(self.root)
        top.grab_set()
        
        # Центрирование
        x = self.root.winfo_x() + (self.root.winfo_width()//2) - (width//2)
        y = self.root.winfo_y() + (self.root.winfo_height()//2) - (height//2)
        top.geometry(f"+{x}+{y}")
        return top

    # --- ДИАЛОГ: УСТРОЙСТВО ---
    def _open_device_dialog(self, device_id=None):
        is_edit = device_id is not None
        title = "Редактирование ТСД" if is_edit else "Новое устройство"
        dlg = self._create_modal(title, 500, 450)
        
        # Поля
        fields = {}
        content = tk.Frame(dlg, bg=COLORS["bg_card"], padx=30, pady=20)
        content.pack(fill="both", expand=True)
        
        # Заголовок
        tk.Label(content, text=title, font=FONTS["h2"], bg=COLORS["bg_card"], fg=COLORS["primary"]).pack(anchor="w", mb=20)

        # Helper для создания полей
        def add_field(label, var_key, options=None):
            f_cont = tk.Frame(content, bg=COLORS["bg_card"])
            f_cont.pack(fill="x", pady=5)
            tk.Label(f_cont, text=label, font=FONTS["body_bold"], bg=COLORS["bg_card"], fg=COLORS["secondary"]).pack(anchor="w")
            
            var = tk.StringVar()
            if options:
                w = ttk.Combobox(f_cont, textvariable=var, values=options, state="readonly", font=FONTS["body"])
            else:
                w = tk.Entry(f_cont, textvariable=var, font=FONTS["body"], bg="#F9FAFB", bd=1, relief="solid")
                # Хак для border color в tk.Entry сложен, используем frame или рельеф
            
            w.pack(fill="x", ipady=6, pady=(5, 0))
            fields[var_key] = var
            return w

        # Списки для комбобоксов
        cur = self.conn.cursor()
        statuses = [r[0] for r in cur.execute("SELECT name FROM statuses").fetchall()]
        locations = [r[0] for r in cur.execute("SELECT name FROM locations").fetchall()]

        add_field("Бренд", "brand")
        add_field("Модель", "model")
        add_field("IMEI", "imei")
        add_field("Статус *", "status", statuses)
        
        # Если редактирование - заполняем
        if is_edit:
            row = cur.execute("SELECT * FROM devices WHERE id=?", (device_id,)).fetchone()
            fields["brand"].set(row["brand"])
            fields["model"].set(row["model"])
            fields["imei"].set(row["imei"])
            # Получаем имя статуса по ID
            st_name = cur.execute("SELECT name FROM statuses WHERE id=?", (row["status_id"],)).fetchone()
            if st_name: fields["status"].set(st_name[0])

        # Кнопки
        btn_area = tk.Frame(dlg, bg="#F9FAFB", height=60)
        btn_area.pack(side="bottom", fill="x")
        
        def save():
            data = {k: v.get().strip() for k, v in fields.items()}
            if not all([data["brand"], data["model"], data["imei"], data["status"]]):
                messagebox.showerror("Ошибка", "Заполните обязательные поля", parent=dlg)
                return
            
            try:
                # Получаем ID статуса
                s_id = cur.execute("SELECT id FROM statuses WHERE name=?", (data["status"],)).fetchone()[0]
                now = datetime.now().strftime("%Y-%m-%d %H:%M")
                
                if is_edit:
                    cur.execute("UPDATE devices SET brand=?, model=?, imei=?, status_id=?, updated_at=? WHERE id=?",
                                (data["brand"], data["model"], data["imei"], s_id, now, device_id))
                else:
                    cur.execute("INSERT INTO devices (brand, model, imei, status_id, updated_at) VALUES (?,?,?,?,?)",
                                (data["brand"], data["model"], data["imei"], s_id, now))
                self.conn.commit()
                self.refresh_all_data()
                dlg.destroy()
            except Exception as e:
                messagebox.showerror("Ошибка БД", str(e), parent=dlg)

        ttk.Button(btn_area, text="Сохранить", style="Primary.TButton", command=save).pack(side="right", padx=20, pady=15)
        ttk.Button(btn_area, text="Отмена", style="Ghost.TButton", command=dlg.destroy).pack(side="right", padx=0, pady=15)

    def _on_registry_double_click(self, event):
        sel = self.tree_reg.selection()
        if not sel: return
        
        item = self.tree_reg.item(sel[0])
        dev_id = item['values'][0]
        self._open_assignment_dialog(dev_id)

    # --- ДИАЛОГ: НАЗНАЧЕНИЕ (ЗАКРЕПЛЕНИЕ) ---
    def _open_assignment_dialog(self, dev_id):
        dlg = self._create_modal("Движение устройства", 500, 480)
        content = tk.Frame(dlg, bg=COLORS["bg_card"], padx=30, pady=20)
        content.pack(fill="both", expand=True)

        cur = self.conn.cursor()
        dev = cur.execute("SELECT * FROM devices WHERE id=?", (dev_id,)).fetchone()
        
        tk.Label(content, text=f"{dev['brand']} {dev['model']}", font=FONTS["h2"], bg=COLORS["bg_card"]).pack(anchor="w")
        tk.Label(content, text=f"IMEI: {dev['imei']}", font=FONTS["body"], fg=COLORS["secondary"], bg=COLORS["bg_card"]).pack(anchor="w", mb=20)

        # Поля формы
        tk.Label(content, text="Сотрудник (ФИО)", bg=COLORS["bg_card"], font=FONTS["body_bold"]).pack(anchor="w", mt=10)
        emp_var = tk.StringVar(value=dev['employee'])
        tk.Entry(content, textvariable=emp_var, font=FONTS["body"], bg="#F9FAFB").pack(fill="x", ipady=6, pady=5)
        
        tk.Label(content, text="Локация", bg=COLORS["bg_card"], font=FONTS["body_bold"]).pack(anchor="w", mt=10)
        locs = [r[0] for r in cur.execute("SELECT name FROM locations").fetchall()]
        loc_var = tk.StringVar()
        cur_loc = cur.execute("SELECT name FROM locations WHERE id=?", (dev['location_id'],)).fetchone()
        if cur_loc: loc_var.set(cur_loc[0])
        ttk.Combobox(content, textvariable=loc_var, values=locs, state="readonly").pack(fill="x", ipady=6, pady=5)
        
        tk.Label(content, text="Новый статус", bg=COLORS["bg_card"], font=FONTS["body_bold"]).pack(anchor="w", mt=10)
        stats = [r[0] for r in cur.execute("SELECT name FROM statuses").fetchall()]
        stat_var = tk.StringVar()
        cur_stat = cur.execute("SELECT name FROM statuses WHERE id=?", (dev['status_id'],)).fetchone()
        if cur_stat: stat_var.set(cur_stat[0])
        ttk.Combobox(content, textvariable=stat_var, values=stats, state="readonly").pack(fill="x", ipady=6, pady=5)

        # Сохранение
        def save_assignment():
            emp = emp_var.get().strip() or "Свободный"
            l_name = loc_var.get()
            s_name = stat_var.get()
            
            try:
                l_id = cur.execute("SELECT id FROM locations WHERE name=?", (l_name,)).fetchone()
                l_id = l_id[0] if l_id else None
                s_id = cur.execute("SELECT id FROM statuses WHERE name=?", (s_name,)).fetchone()
                if not s_id: 
                    messagebox.showerror("Ошибка", "Выберите статус", parent=dlg)
                    return
                
                now = datetime.now().strftime("%Y-%m-%d %H:%M")
                cur.execute("""UPDATE devices SET employee=?, location_id=?, status_id=?, updated_at=? 
                               WHERE id=?""", (emp, l_id, s_id[0], now, dev_id))
                self.conn.commit()
                self.refresh_all_data()
                dlg.destroy()
            except Exception as e:
                messagebox.showerror("Ошибка", str(e))

        ttk.Button(content, text="Применить изменения", style="Primary.TButton", command=save_assignment).pack(fill="x", pady=30)


    # --- ДИАЛОГ: СПРАВОЧНИКИ ---
    def _open_dict_dialog(self, kind, rec_id=None):
        name_map = {"location": "локацию", "status": "статус"}
        table_map = {"location": "locations", "status": "statuses"}
        
        is_edit = rec_id is not None
        title = f"{'Редактировать' if is_edit else 'Добавить'} {name_map[kind]}"
        
        dlg = self._create_modal(title, 400, 250)
        content = tk.Frame(dlg, bg=COLORS["bg_card"], padx=20, pady=20)
        content.pack(fill="both", expand=True)
        
        tk.Label(content, text="Название", bg=COLORS["bg_card"], font=FONTS["body_bold"]).pack(anchor="w")
        var = tk.StringVar()
        e = tk.Entry(content, textvariable=var, font=FONTS["body"], bg="#F9FAFB")
        e.pack(fill="x", ipady=6, pady=5)
        e.focus_set()

        if is_edit:
            cur = self.conn.cursor()
            val = cur.execute(f"SELECT name FROM {table_map[kind]} WHERE id=?", (rec_id,)).fetchone()
            if val: var.set(val[0])

        def save():
            val = var.get().strip()
            if not val: return
            try:
                cur = self.conn.cursor()
                if is_edit:
                    cur.execute(f"UPDATE {table_map[kind]} SET name=? WHERE id=?", (val, rec_id))
                else:
                    cur.execute(f"INSERT INTO {table_map[kind]} (name) VALUES (?)", (val,))
                self.conn.commit()
                self.refresh_all_data()
                dlg.destroy()
            except sqlite3.IntegrityError:
                messagebox.showerror("Ошибка", "Такое имя уже существует", parent=dlg)

        ttk.Button(content, text="Сохранить", style="Primary.TButton", command=save).pack(side="bottom", fill="x")

    def _action_dict(self, kind, action):
        tree = self.tree_loc if kind == "location" else self.tree_stat
        sel = tree.selection()
        if not sel: return
        item_id = tree.item(sel[0])['values'][0]
        
        if action == "edit":
            self._open_dict_dialog(kind, item_id)
        elif action == "delete":
            if messagebox.askyesno("Удаление", "Удалить запись? Ссылки в устройствах будут очищены."):
                cur = self.conn.cursor()
                tbl = "locations" if kind == "location" else "statuses"
                col = "location_id" if kind == "location" else "status_id"
                cur.execute(f"UPDATE devices SET {col}=NULL WHERE {col}=?", (item_id,))
                cur.execute(f"DELETE FROM {tbl} WHERE id=?", (item_id,))
                self.conn.commit()
                self.refresh_all_data()

    def _edit_selected_device_simple(self):
        sel = self.tree_dev_s.selection()
        if sel:
            self._open_device_dialog(self.tree_dev_s.item(sel[0])['values'][0])

    def _delete_selected_device_simple(self):
        sel = self.tree_dev_s.selection()
        if sel:
            d_id = self.tree_dev_s.item(sel[0])['values'][0]
            if messagebox.askyesno("Удаление", "Удалить устройство навсегда?"):
                self.conn.execute("DELETE FROM devices WHERE id=?", (d_id,))
                self.conn.commit()
                self.refresh_all_data()

    # ─── ХЕЛПЕРЫ ───────────────────────────────────────────────────
    def _toggle_fullscreen(self, event=None):
        self.is_fullscreen = not self.is_fullscreen
        self.root.attributes("-fullscreen", self.is_fullscreen)

    def _exit_fullscreen(self, event=None):
        self.is_fullscreen = False
        self.root.attributes("-fullscreen", False)


if __name__ == "__main__":
    root = tk.Tk()
    # Убираем размытость на Windows HighDPI мониторах
    try:
        from ctypes import windll
        windll.shcore.SetProcessDpiAwareness(1)
    except:
        pass
    
    app = TSDRegistryApp(root)
    root.mainloop()
