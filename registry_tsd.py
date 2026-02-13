import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
import math

DB_FILE = "tsd_registry.db"


class RoundedFrame(tk.Canvas):
    """Канвас, имитирующий панель с закруглёнными углами и тенью."""

    def __init__(self, parent, bg_color="#ffffff", corner_radius=16,
                 shadow_color="#e2e8f0", shadow_offset=3, **kw):
        self.bg_color = bg_color
        self.corner_radius = corner_radius
        self.shadow_color = shadow_color
        self.shadow_offset = shadow_offset
        super().__init__(parent, highlightthickness=0, bg=parent["bg"], **kw)
        self.inner_frame = tk.Frame(self, bg=bg_color)
        self.bind("<Configure>", self._redraw)

    def _round_rect(self, x1, y1, x2, y2, r, **kw):
        points = [
            x1 + r, y1,
            x2 - r, y1,
            x2, y1, x2, y1 + r,
            x2, y2 - r,
            x2, y2, x2 - r, y2,
            x1 + r, y2,
            x1, y2, x1, y2 - r,
            x1, y1 + r,
            x1, y1, x1 + r, y1,
        ]
        return self.create_polygon(points, smooth=True, **kw)

    def _redraw(self, _event=None):
        self.delete("all")
        w = self.winfo_width()
        h = self.winfo_height()
        r = self.corner_radius
        s = self.shadow_offset
        # тень
        self._round_rect(s, s, w - 1, h - 1, r, fill=self.shadow_color,
                         outline=self.shadow_color)
        # основная панель
        self._round_rect(0, 0, w - s - 1, h - s - 1, r,
                         fill=self.bg_color, outline=self.bg_color)
        self.create_window(r, r, window=self.inner_frame, anchor="nw",
                           width=w - s - 2 * r, height=h - s - 2 * r)


class TSDRegistryApp:
    # ─── палитра ───────────────────────────────────────────────
    PALETTE = {
        # фоны
        "bg":              "#f0f4f8",
        "surface":         "#ffffff",
        "surface_hover":   "#f7fafc",
        "sidebar":         "#1e293b",
        "sidebar_hover":   "#334155",
        "sidebar_active":  "#3b82f6",
        # текст
        "text":            "#1e293b",
        "text_secondary":  "#64748b",
        "text_on_dark":    "#e2e8f0",
        "text_on_accent":  "#ffffff",
        # акценты
        "accent":          "#3b82f6",
        "accent_hover":    "#2563eb",
        "accent_light":    "#dbeafe",
        "success":         "#10b981",
        "success_light":   "#d1fae5",
        "warning":         "#f59e0b",
        "warning_light":   "#fef3c7",
        "danger":          "#ef4444",
        "danger_light":    "#fee2e2",
        # декор
        "border":          "#e2e8f0",
        "shadow":          "#cbd5e1",
        "divider":         "#f1f5f9",
        # таблица
        "tree_head_bg":    "#f8fafc",
        "tree_sel":        "#dbeafe",
        "tree_stripe":     "#f8fafc",
    }
    FONT_FAMILY = "Segoe UI"

    # ─── init ──────────────────────────────────────────────────
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("TSD Registry — Реєстр ТСД")
        self.root.geometry("1440x860")
        self.root.minsize(1024, 640)
        self.is_fullscreen = False
        self.current_page = "registry"

        self.conn = sqlite3.connect(DB_FILE)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

        self.root.configure(bg=self.PALETTE["bg"])
        self._apply_styles()
        self._build_layout()
        self._show_page("registry")
        self.refresh_all()

        self.root.bind("<F11>", lambda e: self._toggle_fullscreen())
        self.root.bind("<Escape>", lambda e: self._exit_fullscreen())
        self.root.bind("<Configure>", self._on_resize)

    # ─── база данных ───────────────────────────────────────────
    def _init_db(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS statuses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS devices (
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
        cur.execute("SELECT COUNT(*) AS cnt FROM statuses")
        if cur.fetchone()["cnt"] == 0:
            cur.executemany("INSERT INTO statuses(name) VALUES(?)",
                            [("Рабочий",), ("В ремонте",), ("Списан",)])
        self.conn.commit()

    # ─── стили ─────────────────────────────────────────────────
    def _apply_styles(self):
        s = ttk.Style()
        s.theme_use("clam")
        P = self.PALETTE
        F = self.FONT_FAMILY

        s.configure("TFrame", background=P["bg"])
        s.configure("Surface.TFrame", background=P["surface"])
        s.configure("Sidebar.TFrame", background=P["sidebar"])

        s.configure("TLabel", background=P["surface"], foreground=P["text"],
                     font=(F, 10))
        s.configure("BG.TLabel", background=P["bg"], foreground=P["text"],
                     font=(F, 10))
        s.configure("Title.TLabel", background=P["bg"],
                     foreground=P["text"], font=(F, 20, "bold"))
        s.configure("Subtitle.TLabel", background=P["bg"],
                     foreground=P["text_secondary"], font=(F, 10))
        s.configure("CardTitle.TLabel", background=P["surface"],
                     foreground=P["text"], font=(F, 13, "bold"))
        s.configure("CardSub.TLabel", background=P["surface"],
                     foreground=P["text_secondary"], font=(F, 9))
        s.configure("SideLabel.TLabel", background=P["sidebar"],
                     foreground=P["text_on_dark"], font=(F, 11))
        s.configure("SideLabelBold.TLabel", background=P["sidebar"],
                     foreground="#ffffff", font=(F, 14, "bold"))
        s.configure("StatNum.TLabel", background=P["surface"],
                     foreground=P["accent"], font=(F, 28, "bold"))
        s.configure("StatCaption.TLabel", background=P["surface"],
                     foreground=P["text_secondary"], font=(F, 10))

        # кнопки
        s.configure("Accent.TButton", font=(F, 10, "bold"),
                     padding=(16, 9), foreground="white",
                     background=P["accent"], borderwidth=0, relief="flat")
        s.map("Accent.TButton",
              background=[("active", P["accent_hover"]),
                          ("disabled", "#94a3b8")])

        s.configure("Ghost.TButton", font=(F, 10), padding=(14, 8),
                     foreground=P["text"], background=P["divider"],
                     borderwidth=0, relief="flat")
        s.map("Ghost.TButton",
              background=[("active", P["border"])])

        s.configure("Danger.TButton", font=(F, 10, "bold"),
                     padding=(14, 8), foreground="white",
                     background=P["danger"], borderwidth=0, relief="flat")
        s.map("Danger.TButton",
              background=[("active", "#dc2626")])

        s.configure("SideBtn.TButton", font=(F, 11), padding=(14, 12),
                     foreground=P["text_on_dark"],
                     background=P["sidebar"], borderwidth=0,
                     relief="flat", anchor="w")
        s.map("SideBtn.TButton",
              background=[("active", P["sidebar_hover"])])

        s.configure("SideBtnActive.TButton", font=(F, 11, "bold"),
                     padding=(14, 12), foreground="#ffffff",
                     background=P["sidebar_active"], borderwidth=0,
                     relief="flat", anchor="w")
        s.map("SideBtnActive.TButton",
              background=[("active", P["accent_hover"])])

        # таблицы
        s.configure("Treeview", background="white",
                     foreground=P["text"], fieldbackground="white",
                     bordercolor=P["border"], borderwidth=1,
                     rowheight=36, font=(F, 10))
        s.configure("Treeview.Heading", background=P["tree_head_bg"],
                     foreground=P["text"], font=(F, 10, "bold"),
                     relief="flat", borderwidth=0, padding=(8, 6))
        s.map("Treeview",
              background=[("selected", P["tree_sel"])],
              foreground=[("selected", P["text"])])
        s.layout("Treeview", [("Treeview.treearea", {"sticky": "nswe"})])

        # полоса прокрутки
        s.configure("Round.Vertical.TScrollbar",
                     gripcount=0, background=P["border"],
                     troughcolor=P["divider"], borderwidth=0,
                     relief="flat", width=8)
        s.map("Round.Vertical.TScrollbar",
              background=[("active", P["shadow"])])

        # записи
        s.configure("TEntry", padding=(10, 8), font=(F, 10),
                     relief="flat", borderwidth=1,
                     fieldbackground="#f8fafc")
        s.map("TEntry", bordercolor=[("focus", P["accent"])])

        s.configure("TCombobox", padding=(10, 8), font=(F, 10),
                     relief="flat", borderwidth=1,
                     fieldbackground="#f8fafc")

    # ─── скелет интерфейса ─────────────────────────────────────
    def _build_layout(self):
        self.main_container = tk.Frame(self.root, bg=self.PALETTE["bg"])
        self.main_container.pack(fill="both", expand=True)

        # боковая панель
        self.sidebar = tk.Frame(self.main_container,
                                bg=self.PALETTE["sidebar"], width=220)
        self.sidebar.pack(side="left", fill="y")
        self.sidebar.pack_propagate(False)
        self._build_sidebar()

        # область контента
        self.content_area = tk.Frame(self.main_container,
                                     bg=self.PALETTE["bg"])
        self.content_area.pack(side="left", fill="both", expand=True)

        self._build_topbar()

        self.page_container = tk.Frame(self.content_area,
                                       bg=self.PALETTE["bg"])
        self.page_container.pack(fill="both", expand=True, padx=24, pady=(0, 24))

        # страницы
        self.pages = {}
        for name in ("registry", "catalog", "stats"):
            f = tk.Frame(self.page_container, bg=self.PALETTE["bg"])
            self.pages[name] = f

        self._build_page_registry()
        self._build_page_catalog()
        self._build_page_stats()

    # ── боковая панель ─────────────────────────────────────────
    def _build_sidebar(self):
        P = self.PALETTE
        sb = self.sidebar

        # логотип
        logo_frame = tk.Frame(sb, bg=P["sidebar"])
        logo_frame.pack(fill="x", pady=(28, 32), padx=20)

        logo_icon = tk.Canvas(logo_frame, width=40, height=40,
                              bg=P["sidebar"], highlightthickness=0)
        logo_icon.pack(side="left")
        logo_icon.create_oval(4, 4, 36, 36, fill=P["accent"],
                              outline=P["accent"])
        logo_icon.create_text(20, 20, text="T", fill="white",
                              font=(self.FONT_FAMILY, 16, "bold"))

        ttk.Label(logo_frame, text="TSD Registry",
                  style="SideLabelBold.TLabel").pack(side="left", padx=(12, 0))

        # разделитель
        tk.Frame(sb, bg="#334155", height=1).pack(fill="x", padx=16, pady=(0, 16))

        # навигационные кнопки
        self.nav_buttons = {}
        nav_items = [
            ("registry", "📋  Реестр"),
            ("catalog",  "📁  Справочник"),
            ("stats",    "📊  Статистика"),
        ]
        for key, label in nav_items:
            btn = tk.Button(
                sb, text=label, anchor="w",
                font=(self.FONT_FAMILY, 11),
                fg=P["text_on_dark"], bg=P["sidebar"],
                activebackground=P["sidebar_hover"],
                activeforeground="#ffffff",
                bd=0, padx=24, pady=12, cursor="hand2",
                command=lambda k=key: self._show_page(k),
            )
            btn.pack(fill="x")
            btn.bind("<Enter>",
                     lambda e, b=btn: b.config(bg=P["sidebar_hover"])
                     if b != self.nav_buttons.get(self.current_page) else None)
            btn.bind("<Leave>",
                     lambda e, b=btn: b.config(bg=P["sidebar"])
                     if b != self.nav_buttons.get(self.current_page) else None)
            self.nav_buttons[key] = btn

        # нижняя часть
        spacer = tk.Frame(sb, bg=P["sidebar"])
        spacer.pack(fill="both", expand=True)

        tk.Frame(sb, bg="#334155", height=1).pack(fill="x", padx=16, pady=(0, 8))

        fs_btn = tk.Button(
            sb, text="⛶  Полный экран", anchor="w",
            font=(self.FONT_FAMILY, 10), fg=P["text_on_dark"],
            bg=P["sidebar"], activebackground=P["sidebar_hover"],
            activeforeground="#ffffff", bd=0, padx=24, pady=10,
            cursor="hand2", command=self._toggle_fullscreen,
        )
        fs_btn.pack(fill="x", pady=(0, 16))
        fs_btn.bind("<Enter>", lambda e: fs_btn.config(bg=P["sidebar_hover"]))
        fs_btn.bind("<Leave>", lambda e: fs_btn.config(bg=P["sidebar"]))

    def _build_topbar(self):
        P = self.PALETTE
        bar = tk.Frame(self.content_area, bg=P["bg"], height=72)
        bar.pack(fill="x", padx=24, pady=(20, 12))
        bar.pack_propagate(False)

        self.topbar_title = ttk.Label(bar, text="Реестр", style="Title.TLabel")
        self.topbar_title.pack(side="left", anchor="s", pady=(0, 4))

        self.topbar_subtitle = ttk.Label(
            bar, text="Учёт терминалов сбора данных",
            style="Subtitle.TLabel")
        self.topbar_subtitle.pack(side="left", anchor="s", padx=(16, 0),
                                  pady=(0, 6))

        btn_frame = tk.Frame(bar, bg=P["bg"])
        btn_frame.pack(side="right", anchor="s", pady=(0, 4))

        ttk.Button(btn_frame, text="🔄  Обновить", style="Accent.TButton",
                   command=self.refresh_all).pack(side="right")

    # ── переключение страниц ───────────────────────────────────
    def _show_page(self, name: str):
        P = self.PALETTE
        self.current_page = name

        for key, btn in self.nav_buttons.items():
            if key == name:
                btn.config(bg=P["sidebar_active"], fg="#ffffff",
                           font=(self.FONT_FAMILY, 11, "bold"))
            else:
                btn.config(bg=P["sidebar"], fg=P["text_on_dark"],
                           font=(self.FONT_FAMILY, 11))

        titles = {"registry": "Реестр", "catalog": "Справочник",
                  "stats": "Статистика"}
        subtitles = {
            "registry": "Все ТСД в системе — двойной клик для редактирования",
            "catalog": "Управление справочниками",
            "stats": "Сводная аналитика",
        }
        self.topbar_title.config(text=titles[name])
        self.topbar_subtitle.config(text=subtitles[name])

        for pg in self.pages.values():
            pg.pack_forget()
        self.pages[name].pack(fill="both", expand=True)

    # ═══════════════════════════════════════════════════════════
    #  СТРАНИЦА «РЕЕСТР»
    # ═══════════════════════════════════════════════════════════
    def _build_page_registry(self):
        page = self.pages["registry"]
        P = self.PALETTE

        # карточка‑обёртка
        card = tk.Frame(page, bg=P["surface"], bd=0,
                        highlightbackground=P["border"],
                        highlightthickness=1)
        card.pack(fill="both", expand=True)

        # заголовок карточки
        hdr = tk.Frame(card, bg=P["surface"])
        hdr.pack(fill="x", padx=20, pady=(16, 0))

        ttk.Label(hdr, text="Все терминалы", style="CardTitle.TLabel")\
            .pack(side="left")
        ttk.Label(hdr, text="Двойной клик — закрепить / изменить",
                  style="CardSub.TLabel").pack(side="left", padx=(12, 0))

        # поиск
        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", lambda *_: self._filter_registry())

        search_frame = tk.Frame(hdr, bg=P["surface"])
        search_frame.pack(side="right")
        tk.Label(search_frame, text="🔍", bg=P["surface"],
                 font=(self.FONT_FAMILY, 12)).pack(side="left")
        self.search_entry = tk.Entry(
            search_frame, textvariable=self.search_var,
            font=(self.FONT_FAMILY, 10), bg="#f8fafc",
            fg=P["text"], relief="flat", bd=0, width=28,
            insertbackground=P["accent"])
        self.search_entry.pack(side="left", ipady=6, padx=(4, 0))
        self.search_entry.insert(0, "")
        self.search_entry.bind("<FocusIn>", lambda e: None)

        tk.Frame(card, bg=P["border"], height=1).pack(fill="x",
                                                       padx=20, pady=(14, 0))

        # таблица
        tree_frame = tk.Frame(card, bg=P["surface"])
        tree_frame.pack(fill="both", expand=True, padx=2, pady=(0, 2))

        columns = ("id", "brand", "model", "imei", "status",
                   "employee", "location", "updated")
        self.registry_tree = ttk.Treeview(
            tree_frame, columns=columns, show="headings", selectmode="browse")

        headers = {"id": "#", "brand": "Бренд", "model": "Модель",
                   "imei": "IMEI", "status": "Состояние",
                   "employee": "Сотрудник", "location": "Локация",
                   "updated": "Обновлено"}
        min_w = {"id": 50, "brand": 100, "model": 130, "imei": 160,
                 "status": 110, "employee": 150, "location": 150,
                 "updated": 155}

        for col in columns:
            self.registry_tree.heading(col, text=headers[col],
                                       anchor="w")
            self.registry_tree.column(col, width=min_w[col],
                                      minwidth=min_w[col], anchor="w",
                                      stretch=True)

        vsb = ttk.Scrollbar(tree_frame, orient="vertical",
                             command=self.registry_tree.yview,
                             style="Round.Vertical.TScrollbar")
        self.registry_tree.configure(yscrollcommand=vsb.set)

        self.registry_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        self.registry_tree.bind("<Double-1>", self._open_assignment_dialog)

        # чередование строк
        self.registry_tree.tag_configure("stripe",
                                         background=P["tree_stripe"])
        self.registry_tree.tag_configure("normal", background="white")

    def _filter_registry(self):
        query = self.search_var.get().lower().strip()
        self._load_registry(query)

    # ═══════════════════════════════════════════════════════════
    #  СТРАНИЦА «СПРАВОЧНИК»
    # ═══════════════════════════════════════════════════════════
    def _build_page_catalog(self):
        page = self.pages["catalog"]
        P = self.PALETTE

        # три колонки через grid (адаптивно)
        page.columnconfigure(0, weight=3)
        page.columnconfigure(1, weight=2)
        page.columnconfigure(2, weight=2)
        page.rowconfigure(0, weight=1)

        # -- ТСД --
        self.devices_card = self._make_card(page, "Терминалы (ТСД)")
        self.devices_card.grid(row=0, column=0, sticky="nsew",
                               padx=(0, 12), pady=0)
        btn_bar = tk.Frame(self.devices_card.inner, bg=P["surface"])
        btn_bar.pack(fill="x", pady=(0, 8))
        ttk.Button(btn_bar, text="＋ Добавить", style="Accent.TButton",
                   command=self._open_device_dialog).pack(side="left")
        ttk.Button(btn_bar, text="✎ Редактировать", style="Ghost.TButton",
                   command=self._edit_selected_device)\
            .pack(side="left", padx=(8, 0))

        cols_d = ("id", "brand", "model", "imei", "status")
        self.devices_tree = self._make_tree(
            self.devices_card.inner, cols_d,
            {"id": "#", "brand": "Бренд", "model": "Модель",
             "imei": "IMEI", "status": "Состояние"},
            {"id": 45, "brand": 100, "model": 130, "imei": 160,
             "status": 100})

        # -- Локации --
        self.locations_card = self._make_card(page, "Локации")
        self.locations_card.grid(row=0, column=1, sticky="nsew",
                                 padx=(0, 12), pady=0)
        btn_bar2 = tk.Frame(self.locations_card.inner, bg=P["surface"])
        btn_bar2.pack(fill="x", pady=(0, 8))
        ttk.Button(btn_bar2, text="＋", style="Accent.TButton",
                   command=lambda: self._open_dict_dialog("location"))\
            .pack(side="left")
        ttk.Button(btn_bar2, text="✎", style="Ghost.TButton",
                   command=lambda: self._edit_dict("location"))\
            .pack(side="left", padx=(8, 0))

        self.locations_tree = self._make_tree(
            self.locations_card.inner, ("id", "name"),
            {"id": "#", "name": "Название"},
            {"id": 45, "name": 200})

        # -- Состояния --
        self.statuses_card = self._make_card(page, "Состояния")
        self.statuses_card.grid(row=0, column=2, sticky="nsew", pady=0)
        btn_bar3 = tk.Frame(self.statuses_card.inner, bg=P["surface"])
        btn_bar3.pack(fill="x", pady=(0, 8))
        ttk.Button(btn_bar3, text="＋", style="Accent.TButton",
                   command=lambda: self._open_dict_dialog("status"))\
            .pack(side="left")
        ttk.Button(btn_bar3, text="✎", style="Ghost.TButton",
                   command=lambda: self._edit_dict("status"))\
            .pack(side="left", padx=(8, 0))

        self.statuses_tree = self._make_tree(
            self.statuses_card.inner, ("id", "name"),
            {"id": "#", "name": "Название"},
            {"id": 45, "name": 200})

    # ═══════════════════════════════════════════════════════════
    #  СТРАНИЦА «СТАТИСТИКА»
    # ═══════════════════════════════════════════════════════════
    def _build_page_stats(self):
        page = self.pages["stats"]
        P = self.PALETTE

        # верхняя полоска с карточками‑числами
        self.stats_top = tk.Frame(page, bg=P["bg"])
        self.stats_top.pack(fill="x", pady=(0, 16))

        self.stat_cards = {}
        for key, label in [("total", "Всего ТСД"),
                           ("assigned", "Закреплено"),
                           ("free", "Свободных"),
                           ("repair", "В ремонте")]:
            c = self._stat_number_card(self.stats_top, "0", label)
            c.pack(side="left", fill="x", expand=True,
                   padx=(0, 12) if key != "repair" else 0)
            self.stat_cards[key] = c

        # детальный текст
        detail_card = tk.Frame(page, bg=P["surface"], bd=0,
                               highlightbackground=P["border"],
                               highlightthickness=1)
        detail_card.pack(fill="both", expand=True)

        hdr = tk.Frame(detail_card, bg=P["surface"])
        hdr.pack(fill="x", padx=20, pady=(14, 0))
        ttk.Label(hdr, text="Подробная сводка",
                  style="CardTitle.TLabel").pack(side="left")
        tk.Frame(detail_card, bg=P["border"], height=1)\
            .pack(fill="x", padx=20, pady=(12, 0))

        self.stats_text = tk.Text(
            detail_card, wrap="word",
            font=(self.FONT_FAMILY, 11), bg=P["surface"],
            fg=P["text"], relief="flat", bd=0, padx=22, pady=14,
            insertbackground=P["accent"], selectbackground=P["accent_light"])
        self.stats_text.pack(fill="both", expand=True)
        self.stats_text.configure(state="disabled")

    # ── вспомогательные виджеты ────────────────────────────────
    class _CardProxy:
        """Обёртка для карточки, чтобы хранить ссылку на inner frame."""
        def __init__(self, outer, inner):
            self.outer = outer
            self.inner = inner
        def grid(self, **kw): self.outer.grid(**kw)
        def pack(self, **kw): self.outer.pack(**kw)

    def _make_card(self, parent, title: str):
        P = self.PALETTE
        outer = tk.Frame(parent, bg=P["surface"], bd=0,
                         highlightbackground=P["border"],
                         highlightthickness=1)
        hdr = tk.Frame(outer, bg=P["surface"])
        hdr.pack(fill="x", padx=16, pady=(14, 0))
        ttk.Label(hdr, text=title, style="CardTitle.TLabel").pack(side="left")
        tk.Frame(outer, bg=P["border"], height=1)\
            .pack(fill="x", padx=16, pady=(10, 0))
        inner = tk.Frame(outer, bg=P["surface"])
        inner.pack(fill="both", expand=True, padx=16, pady=(10, 14))
        return self._CardProxy(outer, inner)

    def _make_tree(self, parent, columns, headers, widths):
        P = self.PALETTE
        frame = tk.Frame(parent, bg=P["surface"])
        frame.pack(fill="both", expand=True)

        tree = ttk.Treeview(frame, columns=columns, show="headings",
                             selectmode="browse")
        for c in columns:
            tree.heading(c, text=headers[c], anchor="w")
            tree.column(c, width=widths[c], minwidth=widths[c],
                        anchor="w", stretch=True)

        vsb = ttk.Scrollbar(frame, orient="vertical",
                             command=tree.yview,
                             style="Round.Vertical.TScrollbar")
        tree.configure(yscrollcommand=vsb.set)
        tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        tree.tag_configure("stripe", background=P["tree_stripe"])
        tree.tag_configure("normal", background="white")
        return tree

    def _stat_number_card(self, parent, number: str, caption: str):
        P = self.PALETTE
        card = tk.Frame(parent, bg=P["surface"], bd=0,
                        highlightbackground=P["border"],
                        highlightthickness=1)
        inner = tk.Frame(card, bg=P["surface"])
        inner.pack(padx=20, pady=16)

        num_label = ttk.Label(inner, text=number, style="StatNum.TLabel")
        num_label.pack(anchor="w")
        cap_label = ttk.Label(inner, text=caption, style="StatCaption.TLabel")
        cap_label.pack(anchor="w", pady=(2, 0))

        card.num_label = num_label
        card.cap_label = cap_label
        return card

    # ═══════════════════════════════════════════════════════════
    #  ЗАГРУЗКА ДАННЫХ
    # ═══════════════════════════════════════════════════════════
    def refresh_all(self):
        self._load_registry()
        self._load_devices()
        self._load_locations()
        self._load_statuses()
        self._load_stats()

    def _clear_tree(self, tree: ttk.Treeview):
        for row in tree.get_children():
            tree.delete(row)

    def _insert_striped(self, tree, values):
        """Вставка с чередованием цвета строк."""
        existing = len(tree.get_children())
        tag = "stripe" if existing % 2 == 1 else "normal"
        tree.insert("", "end", values=values, tags=(tag,))

    def _load_registry(self, search: str = ""):
        self._clear_tree(self.registry_tree)
        cur = self.conn.cursor()
        cur.execute("""
            SELECT d.id, d.brand, d.model, d.imei,
                   COALESCE(s.name, '') AS status,
                   COALESCE(NULLIF(d.employee, ''), 'Свободный') AS employee,
                   COALESCE(l.name, '') AS location,
                   d.updated_at
            FROM devices d
            LEFT JOIN statuses s ON s.id = d.status_id
            LEFT JOIN locations l ON l.id = d.location_id
            ORDER BY d.id DESC
        """)
        for r in cur.fetchall():
            vals = (r["id"], r["brand"], r["model"], r["imei"],
                    r["status"], r["employee"], r["location"],
                    r["updated_at"])
            if search:
                combined = " ".join(str(v).lower() for v in vals)
                if search not in combined:
                    continue
            self._insert_striped(self.registry_tree, vals)

    def _load_devices(self):
        self._clear_tree(self.devices_tree)
        cur = self.conn.cursor()
        cur.execute("""
            SELECT d.id, d.brand, d.model, d.imei,
                   COALESCE(s.name, '') AS status
            FROM devices d
            LEFT JOIN statuses s ON s.id = d.status_id
            ORDER BY d.id DESC
        """)
        for r in cur.fetchall():
            self._insert_striped(self.devices_tree,
                                 (r["id"], r["brand"], r["model"],
                                  r["imei"], r["status"]))

    def _load_locations(self):
        self._clear_tree(self.locations_tree)
        cur = self.conn.cursor()
        cur.execute("SELECT id, name FROM locations ORDER BY name")
        for r in cur.fetchall():
            self._insert_striped(self.locations_tree,
                                 (r["id"], r["name"]))

    def _load_statuses(self):
        self._clear_tree(self.statuses_tree)
        cur = self.conn.cursor()
        cur.execute("SELECT id, name FROM statuses ORDER BY name")
        for r in cur.fetchall():
            self._insert_striped(self.statuses_tree,
                                 (r["id"], r["name"]))

    def _load_stats(self):
        cur = self.conn.cursor()

        cur.execute("SELECT COUNT(*) AS cnt FROM devices")
        total = cur.fetchone()["cnt"]

        cur.execute("""SELECT COUNT(*) AS cnt FROM devices
                       WHERE TRIM(COALESCE(employee, '')) <> ''
                       AND employee <> 'Свободный'""")
        assigned = cur.fetchone()["cnt"]

        free = total - assigned

        cur.execute("""SELECT COUNT(*) AS cnt FROM devices d
                       LEFT JOIN statuses s ON s.id = d.status_id
                       WHERE s.name = 'В ремонте'""")
        repair = cur.fetchone()["cnt"]

        # обновляем карточки
        self.stat_cards["total"].num_label.config(text=str(total))
        self.stat_cards["assigned"].num_label.config(text=str(assigned))
        self.stat_cards["free"].num_label.config(text=str(free))
        self.stat_cards["repair"].num_label.config(text=str(repair))

        # подробная сводка
        cur.execute("""
            SELECT l.name, COUNT(d.id) AS cnt
            FROM locations l
            LEFT JOIN devices d ON d.location_id = l.id
            GROUP BY l.id, l.name HAVING cnt > 0
            ORDER BY l.name
        """)
        by_location = cur.fetchall()

        cur.execute("""
            SELECT COALESCE(s.name, 'Без состояния') AS status_name,
                   COUNT(d.id) AS cnt
            FROM devices d
            LEFT JOIN statuses s ON s.id = d.status_id
            GROUP BY status_name
            ORDER BY cnt DESC, status_name
        """)
        by_status = cur.fetchall()

        cur.execute("""
            SELECT COALESCE(l.name, 'Без локации') AS location_name,
                   COALESCE(s.name, 'Без состояния') AS status_name,
                   COUNT(d.id) AS cnt
            FROM devices d
            LEFT JOIN locations l ON l.id = d.location_id
            LEFT JOIN statuses s ON s.id = d.status_id
            GROUP BY location_name, status_name
            ORDER BY location_name, cnt DESC
        """)
        loc_status = cur.fetchall()

        lines = []
        lines.append("━━━  ОБЩИЕ ПОКАЗАТЕЛИ  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append(f"   Всего ТСД в системе:         {total}")
        lines.append(f"   Закреплено за сотрудниками:   {assigned}")
        lines.append(f"   Свободных:                    {free}")
        lines.append(f"   В ремонте:                    {repair}")
        lines.append("")

        lines.append("━━━  ПО ЛОКАЦИЯМ  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        if by_location:
            max_name = max(len(r["name"]) for r in by_location)
            for r in by_location:
                bar_len = min(r["cnt"] * 3, 40)
                bar = "█" * bar_len
                lines.append(
                    f"   {r['name']:<{max_name}}  {r['cnt']:>4}  {bar}")
        else:
            lines.append("   — нет данных —")
        lines.append("")

        lines.append("━━━  ПО СОСТОЯНИЯМ  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        if by_status:
            max_name = max(len(r["status_name"]) for r in by_status)
            for r in by_status:
                bar_len = min(r["cnt"] * 3, 40)
                bar = "▓" * bar_len
                lines.append(
                    f"   {r['status_name']:<{max_name}}  {r['cnt']:>4}  {bar}")
        else:
            lines.append("   — нет данных —")
        lines.append("")

        lines.append("━━━  ДЕТАЛИЗАЦИЯ ПО ЛОКАЦИЯМ  ━━━━━━━━━━━━━━━━━━━━━━━━")
        if loc_status:
            current_loc = None
            for r in loc_status:
                if r["location_name"] != current_loc:
                    current_loc = r["location_name"]
                    lines.append(f"\n   📍 {current_loc}")
                    lines.append(f"   {'─' * 40}")
                lines.append(
                    f"      • {r['status_name']}: {r['cnt']}")
        else:
            lines.append("   — нет данных —")

        self.stats_text.configure(state="normal")
        self.stats_text.delete("1.0", "end")
        self.stats_text.insert("1.0", "\n".join(lines))
        self.stats_text.configure(state="disabled")

    # ═══════════════════════════════════════════════════════════
    #  ДИАЛОГИ
    # ═══════════════════════════════════════════════════════════

    def _styled_toplevel(self, title, width=500, height=400):
        P = self.PALETTE
        dlg = tk.Toplevel(self.root)
        dlg.title(title)
        dlg.geometry(f"{width}x{height}")
        dlg.configure(bg=P["surface"])
        dlg.transient(self.root)
        dlg.grab_set()
        dlg.resizable(False, False)

        # центрирование
        dlg.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() - width) // 2
        y = self.root.winfo_y() + (self.root.winfo_height() - height) // 2
        dlg.geometry(f"+{x}+{y}")

        return dlg

    def _make_form_row(self, parent, label_text, row, widget_type="entry",
                       var=None, values=None):
        P = self.PALETTE
        ttk.Label(parent, text=label_text, style="TLabel")\
            .grid(row=row, column=0, sticky="w", pady=(0, 14), padx=(0, 16))

        if widget_type == "entry":
            entry = tk.Entry(
                parent, textvariable=var,
                font=(self.FONT_FAMILY, 11), bg="#f8fafc",
                fg=P["text"], relief="flat", bd=1,
                highlightbackground=P["border"],
                highlightcolor=P["accent"], highlightthickness=1,
                insertbackground=P["accent"])
            entry.grid(row=row, column=1, sticky="ew", pady=(0, 14), ipady=6)
            return entry
        elif widget_type == "combo":
            cb = ttk.Combobox(parent, textvariable=var, values=values,
                              state="readonly", font=(self.FONT_FAMILY, 10))
            cb.grid(row=row, column=1, sticky="ew", pady=(0, 14), ipady=4)
            return cb

    # ── Диалог «Добавить / Редактировать ТСД» ─────────────────
    def _open_device_dialog(self, device_id=None):
        editing = device_id is not None
        dlg = self._styled_toplevel(
            "Редактировать ТСД" if editing else "Новый ТСД",
            width=520, height=380)
        P = self.PALETTE

        # заголовок
        hdr = tk.Frame(dlg, bg=P["accent"], height=56)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr,
                 text="✎  Редактирование" if editing else "＋  Новый терминал",
                 bg=P["accent"], fg="white",
                 font=(self.FONT_FAMILY, 14, "bold"))\
            .pack(side="left", padx=20, pady=12)

        form = tk.Frame(dlg, bg=P["surface"])
        form.pack(fill="both", expand=True, padx=28, pady=20)
        form.columnconfigure(1, weight=1)

        brand_var = tk.StringVar()
        model_var = tk.StringVar()
        imei_var = tk.StringVar()
        status_var = tk.StringVar()

        self._make_form_row(form, "Бренд", 0, var=brand_var)
        self._make_form_row(form, "Модель", 1, var=model_var)
        self._make_form_row(form, "IMEI", 2, var=imei_var)
        self._make_form_row(form, "Состояние", 3, widget_type="combo",
                            var=status_var, values=self._get_status_names())

        if editing:
            cur = self.conn.cursor()
            cur.execute("""
                SELECT d.brand, d.model, d.imei,
                       COALESCE(s.name, '') AS status_name
                FROM devices d
                LEFT JOIN statuses s ON s.id = d.status_id
                WHERE d.id = ?
            """, (device_id,))
            row = cur.fetchone()
            if row:
                brand_var.set(row["brand"])
                model_var.set(row["model"])
                imei_var.set(row["imei"])
                status_var.set(row["status_name"])

        # кнопки
        btn_frame = tk.Frame(dlg, bg=P["surface"])
        btn_frame.pack(fill="x", padx=28, pady=(0, 20))

        def save():
            brand = brand_var.get().strip()
            model = model_var.get().strip()
            imei = imei_var.get().strip()
            status_name = status_var.get().strip()
            if not all([brand, model, imei, status_name]):
                messagebox.showerror("Ошибка",
                                     "Заполните все поля.", parent=dlg)
                return
            status_id = self._get_status_id(status_name)
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                cur = self.conn.cursor()
                if editing:
                    cur.execute("""UPDATE devices
                                   SET brand=?, model=?, imei=?,
                                       status_id=?, updated_at=?
                                   WHERE id=?""",
                                (brand, model, imei, status_id,
                                 now, device_id))
                else:
                    cur.execute("""INSERT INTO devices
                                   (brand, model, imei, status_id,
                                    employee, location_id, updated_at)
                                   VALUES(?,?,?,?,?,?,?)""",
                                (brand, model, imei, status_id,
                                 "Свободный", None, now))
                self.conn.commit()
                dlg.destroy()
                self.refresh_all()
            except sqlite3.IntegrityError:
                messagebox.showerror("Ошибка",
                                     "IMEI должен быть уникальным.",
                                     parent=dlg)

        ttk.Button(btn_frame, text="Отмена", style="Ghost.TButton",
                   command=dlg.destroy).pack(side="right", padx=(8, 0))
        ttk.Button(btn_frame, text="💾  Сохранить", style="Accent.TButton",
                   command=save).pack(side="right")

    def _edit_selected_device(self):
        sel = self.devices_tree.selection()
        if not sel:
            messagebox.showinfo("Подсказка",
                                "Выберите ТСД в таблице для редактирования.")
            return
        device_id = int(self.devices_tree.item(sel[0], "values")[0])
        self._open_device_dialog(device_id)

    # ── Диалог «Добавить / Редактировать справочник» ───────────
    def _open_dict_dialog(self, kind: str, record_id=None):
        table = "locations" if kind == "location" else "statuses"
        caption = "локацию" if kind == "location" else "состояние"
        editing = record_id is not None

        dlg = self._styled_toplevel(
            ("Редактировать " if editing else "Добавить ") + caption,
            width=440, height=230)
        P = self.PALETTE

        color = P["success"] if kind == "location" else P["warning"]
        hdr = tk.Frame(dlg, bg=color, height=50)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr,
                 text=("✎  Редактирование" if editing else "＋  Новая запись"),
                 bg=color, fg="white",
                 font=(self.FONT_FAMILY, 13, "bold"))\
            .pack(side="left", padx=20, pady=10)

        form = tk.Frame(dlg, bg=P["surface"])
        form.pack(fill="both", expand=True, padx=28, pady=20)
        form.columnconfigure(1, weight=1)

        name_var = tk.StringVar()
        self._make_form_row(form, f"Название", 0, var=name_var)

        if editing:
            cur = self.conn.cursor()
            cur.execute(f"SELECT name FROM {table} WHERE id=?", (record_id,))
            row = cur.fetchone()
            if row:
                name_var.set(row["name"])

        btn_frame = tk.Frame(dlg, bg=P["surface"])
        btn_frame.pack(fill="x", padx=28, pady=(0, 20))

        def save():
            name = name_var.get().strip()
            if not name:
                messagebox.showerror("Ошибка",
                                     "Название не может быть пустым.",
                                     parent=dlg)
                return
            try:
                cur = self.conn.cursor()
                if editing:
                    cur.execute(f"UPDATE {table} SET name=? WHERE id=?",
                                (name, record_id))
                else:
                    cur.execute(f"INSERT INTO {table}(name) VALUES(?)",
                                (name,))
                self.conn.commit()
                dlg.destroy()
                self.refresh_all()
            except sqlite3.IntegrityError:
                messagebox.showerror("Ошибка",
                                     "Такое значение уже существует.",
                                     parent=dlg)

        ttk.Button(btn_frame, text="Отмена", style="Ghost.TButton",
                   command=dlg.destroy).pack(side="right", padx=(8, 0))
        ttk.Button(btn_frame, text="💾  Сохранить", style="Accent.TButton",
                   command=save).pack(side="right")

    def _edit_dict(self, kind: str):
        tree = self.locations_tree if kind == "location" \
            else self.statuses_tree
        sel = tree.selection()
        if not sel:
            messagebox.showinfo("Подсказка",
                                "Сначала выберите запись в таблице.")
            return
        rec_id = int(tree.item(sel[0], "values")[0])
        self._open_dict_dialog(kind, rec_id)

    # ── Диалог «Закрепление ТСД» ──────────────────────────────
    def _open_assignment_dialog(self, _event=None):
        sel = self.registry_tree.selection()
        if not sel:
            return
        device_id = int(self.registry_tree.item(sel[0], "values")[0])

        cur = self.conn.cursor()
        cur.execute("""
            SELECT d.id, d.brand, d.model, d.imei,
                   COALESCE(s.name, '') AS status_name,
                   COALESCE(l.name, '') AS location_name,
                   COALESCE(NULLIF(d.employee, ''), 'Свободный') AS employee
            FROM devices d
            LEFT JOIN statuses s ON s.id = d.status_id
            LEFT JOIN locations l ON l.id = d.location_id
            WHERE d.id = ?
        """, (device_id,))
        row = cur.fetchone()
        if not row:
            return

        P = self.PALETTE
        dlg = self._styled_toplevel("Закрепление ТСД", 540, 420)

        # шапка
        hdr = tk.Frame(dlg, bg="#6366f1", height=64)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr, text="🔗  Закрепление терминала",
                 bg="#6366f1", fg="white",
                 font=(self.FONT_FAMILY, 14, "bold"))\
            .pack(side="left", padx=20)

        # инфо-полоска
        info_bar = tk.Frame(dlg, bg="#eef2ff")
        info_bar.pack(fill="x", padx=0, pady=0)
        tk.Label(info_bar,
                 text=f"  {row['brand']}  {row['model']}  •  IMEI: {row['imei']}",
                 bg="#eef2ff", fg="#4338ca",
                 font=(self.FONT_FAMILY, 10, "bold"))\
            .pack(anchor="w", padx=20, pady=10)

        form = tk.Frame(dlg, bg=P["surface"])
        form.pack(fill="both", expand=True, padx=28, pady=20)
        form.columnconfigure(1, weight=1)

        employee_var = tk.StringVar(value=row["employee"])
        location_var = tk.StringVar(value=row["location_name"])
        status_var = tk.StringVar(value=row["status_name"])

        self._make_form_row(form, "Сотрудник", 0, var=employee_var)
        self._make_form_row(form, "Локация", 1, widget_type="combo",
                            var=location_var,
                            values=["", *self._get_location_names()])
        self._make_form_row(form, "Состояние *", 2, widget_type="combo",
                            var=status_var,
                            values=self._get_status_names())

        btn_frame = tk.Frame(dlg, bg=P["surface"])
        btn_frame.pack(fill="x", padx=28, pady=(0, 20))

        def save():
            employee = employee_var.get().strip() or "Свободный"
            location_name = location_var.get().strip()
            status_name = status_var.get().strip()

            if not status_name:
                messagebox.showerror("Ошибка",
                                     "Состояние обязательно.",
                                     parent=dlg)
                return

            status_id = self._get_status_id(status_name)
            location_id = self._get_location_id(location_name) \
                if location_name else None
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            cur = self.conn.cursor()
            cur.execute("""UPDATE devices
                           SET employee=?, location_id=?,
                               status_id=?, updated_at=?
                           WHERE id=?""",
                        (employee, location_id, status_id, now, device_id))
            self.conn.commit()
            dlg.destroy()
            self.refresh_all()

        ttk.Button(btn_frame, text="Отмена", style="Ghost.TButton",
                   command=dlg.destroy).pack(side="right", padx=(8, 0))
        ttk.Button(btn_frame, text="💾  Сохранить", style="Accent.TButton",
                   command=save).pack(side="right")

    # ═══════════════════════════════════════════════════════════
    #  ХЕЛПЕРЫ
    # ═══════════════════════════════════════════════════════════
    def _get_status_names(self):
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM statuses ORDER BY name")
        return [r["name"] for r in cur.fetchall()]

    def _get_location_names(self):
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM locations ORDER BY name")
        return [r["name"] for r in cur.fetchall()]

    def _get_status_id(self, name: str):
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM statuses WHERE name=?", (name,))
        row = cur.fetchone()
        return row["id"] if row else None

    def _get_location_id(self, name: str):
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM locations WHERE name=?", (name,))
        row = cur.fetchone()
        return row["id"] if row else None

    def _toggle_fullscreen(self):
        self.is_fullscreen = not self.is_fullscreen
        self.root.attributes("-fullscreen", self.is_fullscreen)

    def _exit_fullscreen(self):
        self.is_fullscreen = False
        self.root.attributes("-fullscreen", False)

    def _on_resize(self, event=None):
        """Адаптивность: скрываем боковую панель на узких окнах."""
        try:
            w = self.root.winfo_width()
            if w < 900:
                if self.sidebar.winfo_ismapped():
                    self.sidebar.pack_forget()
            else:
                if not self.sidebar.winfo_ismapped():
                    self.sidebar.pack(side="left", fill="y",
                                      before=self.content_area)
        except tk.TclError:
            pass


# ═══════════════════════════════════════════════════════════════
#  ЗАПУСК
# ═══════════════════════════════════════════════════════════════
def main():
    root = tk.Tk()
    app = TSDRegistryApp(root)
    root.mainloop()
    app.conn.close()


if __name__ == "__main__":
    main()

