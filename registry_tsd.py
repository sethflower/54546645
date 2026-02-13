import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

DB_FILE = "tsd_registry.db"


class TSDRegistryApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Реєстр ТСД")
        self.root.geometry("1400x820")
        self.root.minsize(1100, 680)
        self.is_fullscreen = False

        self.conn = sqlite3.connect(DB_FILE)
        self.conn.row_factory = sqlite3.Row
        self._init_db()
        self._init_styles()
        self._build_ui()
        self.refresh_all()

    def _init_db(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS statuses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
            """
        )
        cur.execute(
            """
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
            )
            """
        )
        cur.execute("SELECT COUNT(*) AS cnt FROM statuses")
        if cur.fetchone()["cnt"] == 0:
            cur.executemany(
                "INSERT INTO statuses(name) VALUES(?)",
                [("Рабочий",), ("В ремонте",), ("Списан",)],
            )
        self.conn.commit()

    def _init_styles(self):
        style = ttk.Style()
        style.theme_use("clam")

        self.colors = {
            "bg": "#f5f7fb",
            "panel": "#ffffff",
            "header": "#2d3e50",
            "accent": "#3b82f6",
            "accent_active": "#2563eb",
            "text": "#1f2937",
            "muted": "#6b7280",
            "line": "#d9e2ec",
        }

        self.root.configure(bg=self.colors["bg"])

        style.configure("TFrame", background=self.colors["bg"])
        style.configure("Panel.TFrame", background=self.colors["panel"])
        style.configure("Header.TLabel", background=self.colors["bg"], foreground=self.colors["header"], font=("Segoe UI", 22, "bold"))
        style.configure("SubHeader.TLabel", background=self.colors["bg"], foreground=self.colors["muted"], font=("Segoe UI", 10))
        style.configure("TLabel", background=self.colors["panel"], foreground=self.colors["text"], font=("Segoe UI", 10))

        style.configure("Accent.TButton", font=("Segoe UI", 10, "bold"), padding=(12, 8), foreground="white", background=self.colors["accent"], borderwidth=0)
        style.map("Accent.TButton", background=[("active", self.colors["accent_active"])])

        style.configure("Ghost.TButton", font=("Segoe UI", 10), padding=(10, 7), foreground=self.colors["text"], background="#e8edf6", borderwidth=0)
        style.map("Ghost.TButton", background=[("active", "#dde6f3")])

        style.configure("TNotebook", background=self.colors["bg"], borderwidth=0)
        style.configure("TNotebook.Tab", padding=(14, 10), font=("Segoe UI", 10, "bold"))

        style.configure(
            "Treeview",
            background="white",
            foreground=self.colors["text"],
            fieldbackground="white",
            bordercolor=self.colors["line"],
            rowheight=30,
            font=("Segoe UI", 10),
        )
        style.configure("Treeview.Heading", background="#eef3fb", foreground=self.colors["header"], font=("Segoe UI", 10, "bold"), relief="flat")
        style.map("Treeview", background=[("selected", "#dbeafe")], foreground=[("selected", self.colors["text"])])

    def _build_ui(self):
        wrapper = ttk.Frame(self.root, padding=18)
        wrapper.pack(fill="both", expand=True)

        head = ttk.Frame(wrapper)
        head.pack(fill="x", pady=(0, 12))

        ttk.Label(head, text="Реєстр ТСД", style="Header.TLabel").pack(side="left")
        ttk.Label(head, text="Учет терминалов сбора данных", style="SubHeader.TLabel").pack(side="left", padx=14, pady=8)

        controls = ttk.Frame(head)
        controls.pack(side="right")
        ttk.Button(controls, text="Полный экран", style="Ghost.TButton", command=self.toggle_fullscreen).pack(side="left", padx=4)
        ttk.Button(controls, text="Обновить", style="Accent.TButton", command=self.refresh_all).pack(side="left", padx=4)

        self.notebook = ttk.Notebook(wrapper)
        self.notebook.pack(fill="both", expand=True)

        self.tab_registry = ttk.Frame(self.notebook, style="Panel.TFrame", padding=12)
        self.tab_catalog = ttk.Frame(self.notebook, style="Panel.TFrame", padding=12)
        self.tab_stats = ttk.Frame(self.notebook, style="Panel.TFrame", padding=12)

        self.notebook.add(self.tab_registry, text="Реестр")
        self.notebook.add(self.tab_catalog, text="Справочник")
        self.notebook.add(self.tab_stats, text="Статистика")

        self._build_registry_tab()
        self._build_catalog_tab()
        self._build_stats_tab()

        self.root.bind("<F11>", lambda e: self.toggle_fullscreen())
        self.root.bind("<Escape>", lambda e: self.exit_fullscreen())

    def _build_registry_tab(self):
        top = ttk.Frame(self.tab_registry)
        top.pack(fill="x", pady=(0, 8))
        ttk.Label(top, text="Все ТСД в системе", font=("Segoe UI", 12, "bold")).pack(side="left")
        ttk.Label(top, text="Двойной клик по строке — закрепить за сотрудником/локацией и изменить состояние", foreground=self.colors["muted"]).pack(side="left", padx=12)

        columns = ("id", "brand", "model", "imei", "status", "employee", "location", "updated")
        self.registry_tree = ttk.Treeview(self.tab_registry, columns=columns, show="headings")

        headers = {
            "id": "ID",
            "brand": "Бренд",
            "model": "Модель",
            "imei": "IMEI",
            "status": "Состояние",
            "employee": "Сотрудник",
            "location": "Локация",
            "updated": "Последнее изменение",
        }
        widths = {"id": 55, "brand": 120, "model": 160, "imei": 190, "status": 140, "employee": 180, "location": 180, "updated": 190}
        for col in columns:
            self.registry_tree.heading(col, text=headers[col])
            self.registry_tree.column(col, width=widths[col], anchor="w")

        self.registry_tree.pack(fill="both", expand=True)
        self.registry_tree.bind("<Double-1>", self.open_assignment_dialog)

    def _build_catalog_tab(self):
        main = ttk.Frame(self.tab_catalog)
        main.pack(fill="both", expand=True)

        left = ttk.Frame(main)
        left.pack(side="left", fill="both", expand=True, padx=(0, 8))
        right = ttk.Frame(main)
        right.pack(side="left", fill="both", expand=True)

        # Devices block
        block_devices = ttk.Frame(left, style="Panel.TFrame", padding=10)
        block_devices.pack(fill="both", expand=True)

        head1 = ttk.Frame(block_devices)
        head1.pack(fill="x")
        ttk.Label(head1, text="ТСД", font=("Segoe UI", 12, "bold")).pack(side="left")
        ttk.Button(head1, text="Добавить ТСД", style="Accent.TButton", command=self.open_device_dialog).pack(side="right", padx=4)
        ttk.Button(head1, text="Редактировать", style="Ghost.TButton", command=self.edit_selected_device).pack(side="right", padx=4)

        self.devices_tree = ttk.Treeview(block_devices, columns=("id", "brand", "model", "imei", "status"), show="headings", height=12)
        for col, txt, w in [
            ("id", "ID", 55),
            ("brand", "Бренд", 130),
            ("model", "Модель", 180),
            ("imei", "IMEI", 200),
            ("status", "Состояние", 140),
        ]:
            self.devices_tree.heading(col, text=txt)
            self.devices_tree.column(col, width=w, anchor="w")
        self.devices_tree.pack(fill="both", expand=True, pady=(8, 0))

        # Locations block
        block_locations = ttk.Frame(right, style="Panel.TFrame", padding=10)
        block_locations.pack(fill="both", expand=True, pady=(0, 8))
        h2 = ttk.Frame(block_locations)
        h2.pack(fill="x")
        ttk.Label(h2, text="Локации", font=("Segoe UI", 12, "bold")).pack(side="left")
        ttk.Button(h2, text="Добавить", style="Accent.TButton", command=lambda: self.open_simple_dict_dialog("location")).pack(side="right", padx=4)
        ttk.Button(h2, text="Редактировать", style="Ghost.TButton", command=lambda: self.edit_selected_dict("location")).pack(side="right", padx=4)

        self.locations_tree = ttk.Treeview(block_locations, columns=("id", "name"), show="headings", height=8)
        self.locations_tree.heading("id", text="ID")
        self.locations_tree.heading("name", text="Локация")
        self.locations_tree.column("id", width=60, anchor="w")
        self.locations_tree.column("name", width=260, anchor="w")
        self.locations_tree.pack(fill="both", expand=True, pady=(8, 0))

        # Statuses block
        block_statuses = ttk.Frame(right, style="Panel.TFrame", padding=10)
        block_statuses.pack(fill="both", expand=True)
        h3 = ttk.Frame(block_statuses)
        h3.pack(fill="x")
        ttk.Label(h3, text="Состояния", font=("Segoe UI", 12, "bold")).pack(side="left")
        ttk.Button(h3, text="Добавить", style="Accent.TButton", command=lambda: self.open_simple_dict_dialog("status")).pack(side="right", padx=4)
        ttk.Button(h3, text="Редактировать", style="Ghost.TButton", command=lambda: self.edit_selected_dict("status")).pack(side="right", padx=4)

        self.statuses_tree = ttk.Treeview(block_statuses, columns=("id", "name"), show="headings", height=8)
        self.statuses_tree.heading("id", text="ID")
        self.statuses_tree.heading("name", text="Состояние")
        self.statuses_tree.column("id", width=60, anchor="w")
        self.statuses_tree.column("name", width=260, anchor="w")
        self.statuses_tree.pack(fill="both", expand=True, pady=(8, 0))

    def _build_stats_tab(self):
        ttk.Label(self.tab_stats, text="Общая статистика", font=("Segoe UI", 12, "bold"), background=self.colors["panel"]).pack(anchor="w", pady=(0, 8))
        self.stats_text = tk.Text(self.tab_stats, wrap="word", font=("Consolas", 11), bg="white", fg=self.colors["text"], relief="flat")
        self.stats_text.pack(fill="both", expand=True)
        self.stats_text.configure(state="disabled")

    def refresh_all(self):
        self.load_registry()
        self.load_devices()
        self.load_locations()
        self.load_statuses()
        self.load_stats()

    def _clear_tree(self, tree: ttk.Treeview):
        for row in tree.get_children():
            tree.delete(row)

    def load_registry(self):
        self._clear_tree(self.registry_tree)
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT d.id, d.brand, d.model, d.imei,
                   COALESCE(s.name, '') AS status,
                   COALESCE(NULLIF(d.employee, ''), 'Свободный') AS employee,
                   COALESCE(l.name, '') AS location,
                   d.updated_at
            FROM devices d
            LEFT JOIN statuses s ON s.id = d.status_id
            LEFT JOIN locations l ON l.id = d.location_id
            ORDER BY d.id DESC
            """
        )
        for r in cur.fetchall():
            self.registry_tree.insert("", "end", values=(r["id"], r["brand"], r["model"], r["imei"], r["status"], r["employee"], r["location"], r["updated_at"]))

    def load_devices(self):
        self._clear_tree(self.devices_tree)
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT d.id, d.brand, d.model, d.imei, COALESCE(s.name, '') AS status
            FROM devices d
            LEFT JOIN statuses s ON s.id = d.status_id
            ORDER BY d.id DESC
            """
        )
        for r in cur.fetchall():
            self.devices_tree.insert("", "end", values=(r["id"], r["brand"], r["model"], r["imei"], r["status"]))

    def load_locations(self):
        self._clear_tree(self.locations_tree)
        cur = self.conn.cursor()
        cur.execute("SELECT id, name FROM locations ORDER BY name")
        for r in cur.fetchall():
            self.locations_tree.insert("", "end", values=(r["id"], r["name"]))

    def load_statuses(self):
        self._clear_tree(self.statuses_tree)
        cur = self.conn.cursor()
        cur.execute("SELECT id, name FROM statuses ORDER BY name")
        for r in cur.fetchall():
            self.statuses_tree.insert("", "end", values=(r["id"], r["name"]))

    def load_stats(self):
        cur = self.conn.cursor()

        cur.execute("SELECT COUNT(*) AS cnt FROM devices")
        total_devices = cur.fetchone()["cnt"]

        cur.execute("SELECT COUNT(*) AS cnt FROM devices WHERE TRIM(COALESCE(employee, '')) <> '' AND employee <> 'Свободный'")
        assigned = cur.fetchone()["cnt"]

        cur.execute(
            """
            SELECT l.name, COUNT(d.id) AS cnt
            FROM locations l
            LEFT JOIN devices d ON d.location_id = l.id
            GROUP BY l.id, l.name
            HAVING cnt > 0
            ORDER BY l.name
            """
        )
        by_location = cur.fetchall()

        cur.execute(
            """
            SELECT COALESCE(s.name, 'Без состояния') AS status_name, COUNT(d.id) AS cnt
            FROM devices d
            LEFT JOIN statuses s ON s.id = d.status_id
            GROUP BY status_name
            ORDER BY cnt DESC, status_name
            """
        )
        by_status = cur.fetchall()

        cur.execute(
            """
            SELECT COALESCE(l.name, 'Без локации') AS location_name,
                   COALESCE(s.name, 'Без состояния') AS status_name,
                   COUNT(d.id) AS cnt
            FROM devices d
            LEFT JOIN locations l ON l.id = d.location_id
            LEFT JOIN statuses s ON s.id = d.status_id
            GROUP BY location_name, status_name
            ORDER BY location_name, cnt DESC
            """
        )
        location_status = cur.fetchall()

        lines = []
        lines.append("=== ОБЩИЕ ПОКАЗАТЕЛИ ===")
        lines.append(f"Заведено ТСД в системе: {total_devices}")
        lines.append(f"Закреплено за сотрудниками: {assigned}")
        lines.append("")

        lines.append("=== КОЛ-ВО ТСД ПО ЛОКАЦИЯМ ===")
        if by_location:
            for r in by_location:
                lines.append(f"- {r['name']}: {r['cnt']}")
        else:
            lines.append("- Нет данных")
        lines.append("")

        lines.append("=== КОЛ-ВО ТСД ПО СОСТОЯНИЯМ ===")
        if by_status:
            for r in by_status:
                lines.append(f"- {r['status_name']}: {r['cnt']}")
        else:
            lines.append("- Нет данных")
        lines.append("")

        lines.append("=== СТАТУСЫ ПО КАЖДОЙ ЛОКАЦИИ ===")
        if location_status:
            current_loc = None
            for r in location_status:
                if r["location_name"] != current_loc:
                    current_loc = r["location_name"]
                    lines.append(f"\n{current_loc}:")
                lines.append(f"  • {r['status_name']}: {r['cnt']}")
        else:
            lines.append("- Нет данных")

        self.stats_text.configure(state="normal")
        self.stats_text.delete("1.0", "end")
        self.stats_text.insert("1.0", "\n".join(lines))
        self.stats_text.configure(state="disabled")

    def open_device_dialog(self, device_id=None):
        editing = device_id is not None
        dlg = tk.Toplevel(self.root)
        dlg.title("Редактировать ТСД" if editing else "Добавить ТСД")
        dlg.geometry("470x330")
        dlg.configure(bg=self.colors["panel"])
        dlg.transient(self.root)
        dlg.grab_set()

        form = ttk.Frame(dlg, padding=16, style="Panel.TFrame")
        form.pack(fill="both", expand=True)

        ttk.Label(form, text="Бренд").grid(row=0, column=0, sticky="w", pady=6)
        ttk.Label(form, text="Модель").grid(row=1, column=0, sticky="w", pady=6)
        ttk.Label(form, text="IMEI").grid(row=2, column=0, sticky="w", pady=6)
        ttk.Label(form, text="Состояние").grid(row=3, column=0, sticky="w", pady=6)

        brand_var = tk.StringVar()
        model_var = tk.StringVar()
        imei_var = tk.StringVar()
        status_var = tk.StringVar()

        ttk.Entry(form, textvariable=brand_var, width=34).grid(row=0, column=1, sticky="ew", pady=6)
        ttk.Entry(form, textvariable=model_var, width=34).grid(row=1, column=1, sticky="ew", pady=6)
        ttk.Entry(form, textvariable=imei_var, width=34).grid(row=2, column=1, sticky="ew", pady=6)

        status_values = self.get_status_names()
        cb_status = ttk.Combobox(form, textvariable=status_var, values=status_values, state="readonly", width=32)
        cb_status.grid(row=3, column=1, sticky="ew", pady=6)

        if editing:
            cur = self.conn.cursor()
            cur.execute(
                """
                SELECT d.brand, d.model, d.imei, COALESCE(s.name, '') AS status_name
                FROM devices d
                LEFT JOIN statuses s ON s.id = d.status_id
                WHERE d.id = ?
                """,
                (device_id,),
            )
            row = cur.fetchone()
            if row:
                brand_var.set(row["brand"])
                model_var.set(row["model"])
                imei_var.set(row["imei"])
                status_var.set(row["status_name"])

        def save():
            brand = brand_var.get().strip()
            model = model_var.get().strip()
            imei = imei_var.get().strip()
            status_name = status_var.get().strip()
            if not brand or not model or not imei or not status_name:
                messagebox.showerror("Ошибка", "Заполните все поля ТСД.", parent=dlg)
                return
            status_id = self.get_status_id_by_name(status_name)
            try:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cur = self.conn.cursor()
                if editing:
                    cur.execute(
                        "UPDATE devices SET brand=?, model=?, imei=?, status_id=?, updated_at=? WHERE id=?",
                        (brand, model, imei, status_id, now, device_id),
                    )
                else:
                    cur.execute(
                        "INSERT INTO devices(brand, model, imei, status_id, employee, location_id, updated_at) VALUES(?,?,?,?,?,?,?)",
                        (brand, model, imei, status_id, "Свободный", None, now),
                    )
                self.conn.commit()
                dlg.destroy()
                self.refresh_all()
            except sqlite3.IntegrityError:
                messagebox.showerror("Ошибка", "IMEI должен быть уникальным.", parent=dlg)

        ttk.Button(form, text="Сохранить", style="Accent.TButton", command=save).grid(row=4, column=1, sticky="e", pady=(18, 0))
        form.columnconfigure(1, weight=1)

    def edit_selected_device(self):
        sel = self.devices_tree.selection()
        if not sel:
            messagebox.showwarning("Внимание", "Выберите ТСД для редактирования.")
            return
        device_id = self.devices_tree.item(sel[0], "values")[0]
        self.open_device_dialog(int(device_id))

    def open_simple_dict_dialog(self, kind: str, record_id=None):
        table = "locations" if kind == "location" else "statuses"
        caption = "локацию" if kind == "location" else "состояние"

        dlg = tk.Toplevel(self.root)
        dlg.title(("Редактировать " if record_id else "Добавить ") + caption)
        dlg.geometry("420x190")
        dlg.configure(bg=self.colors["panel"])
        dlg.transient(self.root)
        dlg.grab_set()

        frame = ttk.Frame(dlg, padding=16, style="Panel.TFrame")
        frame.pack(fill="both", expand=True)

        ttk.Label(frame, text=f"Название ({caption})").grid(row=0, column=0, sticky="w", pady=6)
        name_var = tk.StringVar()
        ttk.Entry(frame, textvariable=name_var, width=34).grid(row=1, column=0, sticky="ew", pady=6)

        if record_id:
            cur = self.conn.cursor()
            cur.execute(f"SELECT name FROM {table} WHERE id=?", (record_id,))
            row = cur.fetchone()
            if row:
                name_var.set(row["name"])

        def save():
            name = name_var.get().strip()
            if not name:
                messagebox.showerror("Ошибка", "Название не может быть пустым.", parent=dlg)
                return
            try:
                cur = self.conn.cursor()
                if record_id:
                    cur.execute(f"UPDATE {table} SET name=? WHERE id=?", (name, record_id))
                else:
                    cur.execute(f"INSERT INTO {table}(name) VALUES(?)", (name,))
                self.conn.commit()
                dlg.destroy()
                self.refresh_all()
            except sqlite3.IntegrityError:
                messagebox.showerror("Ошибка", "Такое значение уже существует.", parent=dlg)

        ttk.Button(frame, text="Сохранить", style="Accent.TButton", command=save).grid(row=2, column=0, sticky="e", pady=(16, 0))
        frame.columnconfigure(0, weight=1)

    def edit_selected_dict(self, kind: str):
        tree = self.locations_tree if kind == "location" else self.statuses_tree
        sel = tree.selection()
        if not sel:
            messagebox.showwarning("Внимание", "Сначала выберите запись.")
            return
        rec_id = int(tree.item(sel[0], "values")[0])
        self.open_simple_dict_dialog(kind, rec_id)

    def open_assignment_dialog(self, _event=None):
        sel = self.registry_tree.selection()
        if not sel:
            return
        device_id = int(self.registry_tree.item(sel[0], "values")[0])

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT d.id, d.brand, d.model, d.imei,
                   COALESCE(s.name, '') AS status_name,
                   COALESCE(l.name, '') AS location_name,
                   COALESCE(NULLIF(d.employee, ''), 'Свободный') AS employee
            FROM devices d
            LEFT JOIN statuses s ON s.id = d.status_id
            LEFT JOIN locations l ON l.id = d.location_id
            WHERE d.id = ?
            """,
            (device_id,),
        )
        row = cur.fetchone()
        if not row:
            return

        dlg = tk.Toplevel(self.root)
        dlg.title("Закрепление ТСД")
        dlg.geometry("500x360")
        dlg.configure(bg=self.colors["panel"])
        dlg.transient(self.root)
        dlg.grab_set()

        frame = ttk.Frame(dlg, padding=16, style="Panel.TFrame")
        frame.pack(fill="both", expand=True)

        ttk.Label(frame, text=f"ТСД: {row['brand']} {row['model']} ({row['imei']})", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 12))

        ttk.Label(frame, text="Сотрудник").grid(row=1, column=0, sticky="w", pady=6)
        ttk.Label(frame, text="Локация").grid(row=2, column=0, sticky="w", pady=6)
        ttk.Label(frame, text="Состояние *").grid(row=3, column=0, sticky="w", pady=6)

        employee_var = tk.StringVar(value=row["employee"])
        location_var = tk.StringVar(value=row["location_name"])
        status_var = tk.StringVar(value=row["status_name"])

        ttk.Entry(frame, textvariable=employee_var, width=35).grid(row=1, column=1, sticky="ew", pady=6)
        cb_loc = ttk.Combobox(frame, textvariable=location_var, values=["", *self.get_location_names()], state="readonly", width=33)
        cb_loc.grid(row=2, column=1, sticky="ew", pady=6)
        cb_stat = ttk.Combobox(frame, textvariable=status_var, values=self.get_status_names(), state="readonly", width=33)
        cb_stat.grid(row=3, column=1, sticky="ew", pady=6)

        def save():
            employee = employee_var.get().strip() or "Свободный"
            location_name = location_var.get().strip()
            status_name = status_var.get().strip()

            if not status_name:
                messagebox.showerror("Ошибка", "Состояние обязательно для заполнения.", parent=dlg)
                return

            status_id = self.get_status_id_by_name(status_name)
            location_id = self.get_location_id_by_name(location_name) if location_name else None
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            cur = self.conn.cursor()
            cur.execute(
                "UPDATE devices SET employee=?, location_id=?, status_id=?, updated_at=? WHERE id=?",
                (employee, location_id, status_id, now, device_id),
            )
            self.conn.commit()
            dlg.destroy()
            self.refresh_all()

        ttk.Button(frame, text="Сохранить закрепление", style="Accent.TButton", command=save).grid(row=4, column=1, sticky="e", pady=(20, 0))
        frame.columnconfigure(1, weight=1)

    def get_status_names(self):
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM statuses ORDER BY name")
        return [r["name"] for r in cur.fetchall()]

    def get_location_names(self):
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM locations ORDER BY name")
        return [r["name"] for r in cur.fetchall()]

    def get_status_id_by_name(self, name: str):
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM statuses WHERE name=?", (name,))
        row = cur.fetchone()
        return row["id"] if row else None

    def get_location_id_by_name(self, name: str):
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM locations WHERE name=?", (name,))
        row = cur.fetchone()
        return row["id"] if row else None

    def toggle_fullscreen(self):
        self.is_fullscreen = not self.is_fullscreen
        self.root.attributes("-fullscreen", self.is_fullscreen)

    def exit_fullscreen(self):
        self.is_fullscreen = False
        self.root.attributes("-fullscreen", False)


def main():
    root = tk.Tk()
    app = TSDRegistryApp(root)
    root.mainloop()
    app.conn.close()


if __name__ == "__main__":
    main()
