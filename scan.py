import getpass
import sqlite3
from contextlib import closing
from datetime import datetime
import tkinter as tk
from tkinter import messagebox, simpledialog, ttk

DB_FILE = "wms_3pl.db"


class Database:
    def __init__(self, db_file: str):
        self.conn = sqlite3.connect(db_file)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._create_schema()
        self._migrate_products_table()
        self._migrate_suppliers_table()
        self._migrate_clients_table()
        self._migrate_inbound_tables()
        self._seed_reference_data()

    def _create_schema(self):
        with closing(self.conn.cursor()) as cur:
            cur.executescript(
                """
                CREATE TABLE IF NOT EXISTS clients (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    contact TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS suppliers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE
                );

                CREATE TABLE IF NOT EXISTS warehouses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE
                );

                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE
                );

                CREATE TABLE IF NOT EXISTS subcategories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category_id INTEGER,
                    name TEXT NOT NULL,
                    UNIQUE(category_id, name),
                    FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sku TEXT UNIQUE,
                    name TEXT,
                    client_id INTEGER,
                    unit TEXT NOT NULL DEFAULT 'pcs',
                    min_stock INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(client_id) REFERENCES clients(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS movements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER NOT NULL,
                    movement_type TEXT NOT NULL CHECK(movement_type IN ('IN', 'OUT')),
                    quantity INTEGER NOT NULL CHECK(quantity > 0),
                    reference TEXT,
                    moved_at TEXT NOT NULL,
                    note TEXT,
                    FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS inbound_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_number TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    received_at TEXT,
                    created_by TEXT NOT NULL,
                    supplier_id INTEGER NOT NULL,
                    client_id INTEGER NOT NULL,
                    warehouse_id INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('Новый', 'Принят')),
                    FOREIGN KEY(supplier_id) REFERENCES suppliers(id) ON DELETE RESTRICT,
                    FOREIGN KEY(client_id) REFERENCES clients(id) ON DELETE RESTRICT,
                    FOREIGN KEY(warehouse_id) REFERENCES warehouses(id) ON DELETE RESTRICT
                );

                CREATE TABLE IF NOT EXISTS inbound_order_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL,
                    category_id INTEGER,
                    subcategory_id INTEGER,
                    product_id INTEGER NOT NULL,
                    planned_qty REAL NOT NULL CHECK(planned_qty > 0),
                    actual_qty REAL NOT NULL DEFAULT 0,
                    actual_filled INTEGER NOT NULL DEFAULT 0,
                    planned_weight REAL,
                    planned_volume REAL,
                    serial_numbers TEXT,
                    FOREIGN KEY(order_id) REFERENCES inbound_orders(id) ON DELETE CASCADE,
                    FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE SET NULL,
                    FOREIGN KEY(subcategory_id) REFERENCES subcategories(id) ON DELETE SET NULL,
                    FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE RESTRICT
                );
                """
            )
            self.conn.commit()

    def _add_column_if_missing(self, table: str, column: str, definition: str):
        existing = {row[1] for row in self.query(f"PRAGMA table_info({table})")}
        if column not in existing:
            self.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def _add_unique_index_if_missing(self, table: str, column: str):
        index_name = f"idx_{table}_{column}_unique"
        indexes = self.query(f"PRAGMA index_list({table})")
        if not any(row[1] == index_name for row in indexes):
            self.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table}({column})")

    def _migrate_products_table(self):
        self._add_column_if_missing("products", "brand", "TEXT")
        self._add_column_if_missing("products", "supplier_id", "INTEGER REFERENCES suppliers(id) ON DELETE SET NULL")
        self._add_column_if_missing("products", "volume", "REAL")
        self._add_column_if_missing("products", "weight", "REAL")
        self._add_column_if_missing("products", "barcode", "TEXT")
        self._add_column_if_missing("products", "serial_tracking", "TEXT NOT NULL DEFAULT 'Нет'")
        self._add_column_if_missing("products", "category_id", "INTEGER REFERENCES categories(id) ON DELETE SET NULL")
        self._add_column_if_missing("products", "subcategory_id", "INTEGER REFERENCES subcategories(id) ON DELETE SET NULL")
        self._add_column_if_missing("products", "product_owner", "TEXT")


    def _migrate_suppliers_table(self):
        self._add_column_if_missing("suppliers", "phone", "TEXT")
        self._add_column_if_missing("suppliers", "created_at", "TEXT")
        self.execute(
            """
            UPDATE suppliers
            SET created_at = ?
            WHERE created_at IS NULL OR TRIM(created_at) = ''
            """,
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),),
        )

    def _migrate_clients_table(self):
        self._add_column_if_missing("clients", "code", "TEXT")
        self._add_column_if_missing("clients", "name", "TEXT")
        self._add_column_if_missing("clients", "contact", "TEXT")
        self._add_column_if_missing("clients", "created_at", "TEXT")
        self.execute(
            """
            UPDATE clients
            SET created_at = ?
            WHERE created_at IS NULL OR TRIM(created_at) = ''
            """,
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),),
        )

    def _migrate_inbound_tables(self):
        self._add_column_if_missing("inbound_order_items", "actual_filled", "INTEGER NOT NULL DEFAULT 0")
        self._add_column_if_missing("inbound_order_items", "serial_numbers", "TEXT")

    def _seed_reference_data(self):
        if not self.query("SELECT id FROM suppliers LIMIT 1"):
            self.execute(
                "INSERT INTO suppliers(name, phone, created_at) VALUES(?, ?, ?)",
                ("Default Supplier", "", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )
        if not self.query("SELECT id FROM warehouses LIMIT 1"):
            self.execute("INSERT INTO warehouses(name) VALUES(?)", ("Основной склад",))

    def execute(self, query, params=()):
        with closing(self.conn.cursor()) as cur:
            cur.execute(query, params)
            self.conn.commit()
            return cur.lastrowid

    def query(self, query, params=()):
        with closing(self.conn.cursor()) as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def close(self):
        self.conn.close()


class WMSApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("3PL WMS | Warehouse Management System")
        self.geometry("1280x780")
        self.minsize(1150, 700)

        self.db = Database(DB_FILE)
        self.current_user = getpass.getuser()

        self.style = ttk.Style(self)
        self._configure_style()

        self.selected_copy_value = ""
        self.nomenclature_brand_filter = tk.StringVar()
        self.nomenclature_brand_filter.trace_add("write", lambda *_: self.refresh_nomenclature())
        self.suppliers_search_var = tk.StringVar()
        self.suppliers_search_var.trace_add("write", lambda *_: self.refresh_suppliers())
        self.clients_search_var = tk.StringVar()
        self.clients_search_var.trace_add("write", lambda *_: self.refresh_3pl_clients())
        self.categories_filter_var = tk.StringVar()
        self.categories_filter_var.trace_add("write", lambda *_: self.refresh_categories_tab())
        self.inbound_order_search_var = tk.StringVar()
        self.inbound_order_search_var.trace_add("write", lambda *_: self.refresh_inbound_orders())
        self.inbound_status_var = tk.StringVar(value="Все")
        self.inbound_date_filter_var = tk.StringVar()

        self._build_layout()
        self.refresh_all()
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def _configure_style(self):
        self.configure(bg="#0B172A")
        self.style.theme_use("clam")

        palette = {
            "bg": "#F2F5F9",
            "panel": "#FFFFFF",
            "accent": "#1F4E79",
            "accent2": "#4A90E2",
            "text": "#122033",
            "muted": "#687B91",
        }

        self.style.configure("TFrame", background=palette["bg"])
        self.style.configure("Panel.TFrame", background=palette["panel"])
        self.style.configure("Header.TLabel", background=palette["bg"], foreground=palette["text"], font=("Segoe UI", 20, "bold"))
        self.style.configure("Subtitle.TLabel", background=palette["bg"], foreground=palette["muted"], font=("Segoe UI", 10))
        self.style.configure("Section.TLabel", background=palette["panel"], foreground=palette["text"], font=("Segoe UI", 12, "bold"))
        self.style.configure("TLabel", background=palette["panel"], foreground=palette["text"], font=("Segoe UI", 10))
        self.style.configure("TButton", font=("Segoe UI", 10, "bold"), padding=8, foreground="#FFFFFF", background=palette["accent"], borderwidth=0)
        self.style.map("TButton", background=[("active", palette["accent2"])])

        self.style.configure("Treeview", rowheight=28, font=("Segoe UI", 10))
        self.style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"), background=palette["accent"], foreground="#FFFFFF", padding=6)
        self.style.map("Treeview", background=[("selected", "#D7E8FB")], foreground=[("selected", "#122033")])

    def _build_layout(self):
        root = ttk.Frame(self, padding=18)
        root.pack(fill="both", expand=True)

        header = ttk.Frame(root)
        header.pack(fill="x")

        ttk.Label(header, text="Warehouse Management System (3PL)", style="Header.TLabel").pack(anchor="w")
        ttk.Label(header, text="Складская операционная система: поставщики, 3PL клиенты, категории, номенклатура, приходы товара, движения и остатки", style="Subtitle.TLabel").pack(anchor="w", pady=(2, 14))

        self.metrics_var = tk.StringVar(value="")
        metrics_card = ttk.Frame(root, style="Panel.TFrame", padding=14)
        metrics_card.pack(fill="x", pady=(0, 12))
        ttk.Label(metrics_card, textvariable=self.metrics_var, style="Section.TLabel").pack(anchor="w")

        notebook = ttk.Notebook(root)
        notebook.pack(fill="both", expand=True)

        self.suppliers_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)
        self.clients_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)
        self.categories_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)
        self.nomenclature_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)
        self.inbound_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)
        self.movements_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)
        self.stock_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)

        notebook.add(self.suppliers_tab, text="Поставщики")
        notebook.add(self.clients_tab, text="3PL клиенты")
        notebook.add(self.categories_tab, text="Категории товаров")
        notebook.add(self.nomenclature_tab, text="Номенклатура")
        notebook.add(self.inbound_tab, text="Приходы товара")
        notebook.add(self.movements_tab, text="Движения")
        notebook.add(self.stock_tab, text="Остатки")

        self._build_suppliers_tab()
        self._build_clients_tab()
        self._build_categories_tab()
        self._build_nomenclature_tab()
        self._build_inbound_tab()
        self._build_movements_tab()
        self._build_stock_tab()

    def _build_suppliers_tab(self):
        controls = ttk.Frame(self.suppliers_tab, style="Panel.TFrame")
        controls.pack(fill="x", pady=(0, 10))

        ttk.Label(controls, text="Поиск по поставщику").pack(side="left")
        ttk.Entry(controls, textvariable=self.suppliers_search_var, width=28).pack(side="left", padx=(8, 14))
        ttk.Button(controls, text="Создать нового поставщика", command=self.open_create_supplier_dialog).pack(side="left")
        ttk.Button(controls, text="Редактировать карточку", command=self.open_edit_supplier_dialog).pack(side="left", padx=(10, 0))
        ttk.Button(controls, text="Удалить карточку", command=self.delete_selected_supplier).pack(side="left", padx=(10, 0))

        self.suppliers_tree = ttk.Treeview(self.suppliers_tab, columns=("id", "name", "phone", "created"), show="headings")
        self.suppliers_tree.pack(fill="both", expand=True)

        for col, title, width in [
            ("id", "ID", 70),
            ("name", "Поставщик", 360),
            ("phone", "Телефон", 220),
            ("created", "Создан", 200),
        ]:
            self.suppliers_tree.heading(col, text=title)
            self.suppliers_tree.column(col, width=width, anchor="w")

        self.suppliers_copy_menu = tk.Menu(self, tearoff=0)
        self.suppliers_copy_menu.add_command(label="Скопировать", command=self.copy_selected_value)
        self.suppliers_tree.bind("<Button-3>", self.show_suppliers_copy_menu)

    def _next_supplier_id(self):
        row = self.db.query("SELECT COALESCE(MAX(id), 0) + 1 FROM suppliers")
        return str(row[0][0])

    def open_create_supplier_dialog(self):
        self._open_supplier_dialog(mode="create")

    def open_edit_supplier_dialog(self):
        selected = self.suppliers_tree.selection()
        if not selected:
            messagebox.showwarning("Валидация", "Выберите карточку поставщика для редактирования")
            return
        supplier_id = int(self.suppliers_tree.item(selected[0], "values")[0])
        self._open_supplier_dialog(mode="edit", supplier_id=supplier_id)

    def _open_supplier_dialog(self, mode: str, supplier_id: int | None = None):
        dialog = tk.Toplevel(self)
        dialog.title("Карточка поставщика")
        dialog.geometry("520x280")
        dialog.transient(self)
        dialog.grab_set()

        frm = ttk.Frame(dialog, style="Panel.TFrame", padding=14)
        frm.pack(fill="both", expand=True)

        supplier_name_var = tk.StringVar()
        supplier_id_var = tk.StringVar(value=self._next_supplier_id())
        supplier_contact_var = tk.StringVar()
        created_at_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        if mode == "edit" and supplier_id is not None:
            row = self.db.query("SELECT id, name, phone, created_at FROM suppliers WHERE id = ?", (supplier_id,))
            if not row:
                messagebox.showerror("Ошибка", "Поставщик не найден", parent=dialog)
                dialog.destroy()
                return
            sid, name, phone, created = row[0]
            supplier_id_var.set(str(sid))
            supplier_name_var.set(name or "")
            supplier_contact_var.set(phone or "")
            created_at_var.set(created or "")

        fields = [
            ("Название поставщика", supplier_name_var, False),
            ("ID поставщика", supplier_id_var, True),
            ("Контакт", supplier_contact_var, False),
            ("Дата создания", created_at_var, True),
        ]

        for i, (label, var, readonly) in enumerate(fields):
            ttk.Label(frm, text=label).grid(row=i, column=0, sticky="w", pady=6)
            state = "readonly" if readonly else "normal"
            ttk.Entry(frm, textvariable=var, width=38, state=state).grid(row=i, column=1, sticky="w", pady=6)

        def on_save():
            name = supplier_name_var.get().strip()
            if not name:
                messagebox.showwarning("Валидация", "Название поставщика обязательно", parent=dialog)
                return

            if mode == "create":
                try:
                    self.db.execute(
                        "INSERT INTO suppliers(name, phone, created_at) VALUES(?, ?, ?)",
                        (name, supplier_contact_var.get().strip(), created_at_var.get().strip()),
                    )
                except sqlite3.IntegrityError:
                    messagebox.showerror("Ошибка", "Поставщик с таким названием уже существует", parent=dialog)
                    return
            else:
                self.db.execute(
                    "UPDATE suppliers SET name = ?, phone = ? WHERE id = ?",
                    (name, supplier_contact_var.get().strip(), supplier_id),
                )

            self.refresh_all()
            dialog.destroy()

        ttk.Button(frm, text="Сохранить", command=on_save).grid(row=len(fields), column=1, sticky="e", pady=(12, 0))

    def delete_selected_supplier(self):
        selected = self.suppliers_tree.selection()
        if not selected:
            messagebox.showwarning("Валидация", "Выберите карточку поставщика для удаления")
            return
        supplier_id, supplier_name = self.suppliers_tree.item(selected[0], "values")[:2]
        if not messagebox.askyesno("Подтверждение", f"Удалить поставщика {supplier_name}?"):
            return
        self.db.execute("DELETE FROM suppliers WHERE id = ?", (int(supplier_id),))
        self.refresh_all()

    def show_suppliers_copy_menu(self, event):
        row = self.suppliers_tree.identify_row(event.y)
        col = self.suppliers_tree.identify_column(event.x)
        if not row:
            return
        self.suppliers_tree.selection_set(row)
        values = self.suppliers_tree.item(row, "values")
        col_index = int(col.replace("#", "")) - 1 if col else 0
        self.selected_copy_value = values[col_index] if 0 <= col_index < len(values) else ""
        self.suppliers_copy_menu.tk_popup(event.x_root, event.y_root)

    def _build_clients_tab(self):
        controls = ttk.Frame(self.clients_tab, style="Panel.TFrame")
        controls.pack(fill="x", pady=(0, 10))

        ttk.Label(controls, text="Поиск по 3PL клиенту").pack(side="left")
        ttk.Entry(controls, textvariable=self.clients_search_var, width=28).pack(side="left", padx=(8, 14))
        ttk.Button(controls, text="Создать нового 3PL клиента", command=self.open_create_client_dialog).pack(side="left")
        ttk.Button(controls, text="Редактировать карточку", command=self.open_edit_client_dialog).pack(side="left", padx=(10, 0))
        ttk.Button(controls, text="Удалить карточку", command=self.delete_selected_client).pack(side="left", padx=(10, 0))

        self.clients_tree = ttk.Treeview(self.clients_tab, columns=("id", "name", "contact", "created"), show="headings")
        self.clients_tree.pack(fill="both", expand=True)

        for col, title, width in [
            ("id", "ID", 70),
            ("name", "3PL клиент", 360),
            ("contact", "Телефон", 220),
            ("created", "Создан", 200),
        ]:
            self.clients_tree.heading(col, text=title)
            self.clients_tree.column(col, width=width, anchor="w")

        self.clients_copy_menu = tk.Menu(self, tearoff=0)
        self.clients_copy_menu.add_command(label="Скопировать", command=self.copy_selected_value)
        self.clients_tree.bind("<Button-3>", self.show_clients_copy_menu)

    def _next_client_id(self):
        row = self.db.query("SELECT COALESCE(MAX(id), 0) + 1 FROM clients")
        return str(row[0][0])

    def _next_client_code(self):
        row = self.db.query("SELECT code FROM clients WHERE code LIKE 'C%' ORDER BY id DESC LIMIT 1")
        if not row or not row[0][0]:
            return "C00001"
        code = row[0][0]
        try:
            return f"C{int(code.replace('C', '')) + 1:05d}"
        except ValueError:
            cnt = self.db.query("SELECT COUNT(*) FROM clients")[0][0] + 1
            return f"C{cnt:05d}"

    def open_create_client_dialog(self):
        self._open_client_dialog(mode="create")

    def open_edit_client_dialog(self):
        selected = self.clients_tree.selection()
        if not selected:
            messagebox.showwarning("Валидация", "Выберите карточку 3PL клиента для редактирования")
            return
        client_id = int(self.clients_tree.item(selected[0], "values")[0])
        self._open_client_dialog(mode="edit", client_id=client_id)

    def _open_client_dialog(self, mode: str, client_id: int | None = None):
        dialog = tk.Toplevel(self)
        dialog.title("Карточка 3PL клиента")
        dialog.geometry("540x280")
        dialog.transient(self)
        dialog.grab_set()

        frm = ttk.Frame(dialog, style="Panel.TFrame", padding=14)
        frm.pack(fill="both", expand=True)

        client_name_var = tk.StringVar()
        client_id_var = tk.StringVar(value=self._next_client_id())
        client_contact_var = tk.StringVar()
        created_at_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        if mode == "edit" and client_id is not None:
            row = self.db.query("SELECT id, name, contact, created_at FROM clients WHERE id = ?", (client_id,))
            if not row:
                messagebox.showerror("Ошибка", "3PL клиент не найден", parent=dialog)
                dialog.destroy()
                return
            cid, name, contact, created = row[0]
            client_id_var.set(str(cid))
            client_name_var.set(name or "")
            client_contact_var.set(contact or "")
            created_at_var.set(created or "")

        fields = [
            ("Название 3PL клиента", client_name_var, False),
            ("ID 3PL клиента", client_id_var, True),
            ("Контакт", client_contact_var, False),
            ("Дата создания", created_at_var, True),
        ]

        for i, (label, var, readonly) in enumerate(fields):
            ttk.Label(frm, text=label).grid(row=i, column=0, sticky="w", pady=6)
            state = "readonly" if readonly else "normal"
            ttk.Entry(frm, textvariable=var, width=38, state=state).grid(row=i, column=1, sticky="w", pady=6)

        def on_save():
            name = client_name_var.get().strip()
            if not name:
                messagebox.showwarning("Валидация", "Название 3PL клиента обязательно", parent=dialog)
                return

            if mode == "create":
                try:
                    self.db.execute(
                        "INSERT INTO clients(code, name, contact, created_at) VALUES(?, ?, ?, ?)",
                        (self._next_client_code(), name, client_contact_var.get().strip(), created_at_var.get().strip()),
                    )
                except sqlite3.IntegrityError:
                    messagebox.showerror("Ошибка", "3PL клиент с таким названием уже существует", parent=dialog)
                    return
            else:
                self.db.execute(
                    "UPDATE clients SET name = ?, contact = ? WHERE id = ?",
                    (name, client_contact_var.get().strip(), client_id),
                )

            self.refresh_all()
            dialog.destroy()

        ttk.Button(frm, text="Сохранить", command=on_save).grid(row=len(fields), column=1, sticky="e", pady=(12, 0))

    def delete_selected_client(self):
        selected = self.clients_tree.selection()
        if not selected:
            messagebox.showwarning("Валидация", "Выберите карточку 3PL клиента для удаления")
            return
        client_id, client_name = self.clients_tree.item(selected[0], "values")[:2]
        if not messagebox.askyesno("Подтверждение", f"Удалить 3PL клиента {client_name}?"):
            return
        self.db.execute("DELETE FROM clients WHERE id = ?", (int(client_id),))
        self.refresh_all()

    def show_clients_copy_menu(self, event):
        row = self.clients_tree.identify_row(event.y)
        col = self.clients_tree.identify_column(event.x)
        if not row:
            return
        self.clients_tree.selection_set(row)
        values = self.clients_tree.item(row, "values")
        col_index = int(col.replace("#", "")) - 1 if col else 0
        self.selected_copy_value = values[col_index] if 0 <= col_index < len(values) else ""
        self.clients_copy_menu.tk_popup(event.x_root, event.y_root)

    def _build_categories_tab(self):
        top = ttk.Frame(self.categories_tab, style="Panel.TFrame")
        top.pack(fill="x", pady=(0, 10))
        ttk.Label(top, text="Поиск по категории/подкатегории").pack(side="left")
        ttk.Entry(top, textvariable=self.categories_filter_var, width=30).pack(side="left", padx=(8, 12))

        create_frame = ttk.Frame(self.categories_tab, style="Panel.TFrame")
        create_frame.pack(fill="x", pady=(0, 10))

        self.new_category_var = tk.StringVar()
        self.new_subcategory_var = tk.StringVar()
        self.subcategory_parent_var = tk.StringVar()

        ttk.Label(create_frame, text="Новая категория").grid(row=0, column=0, sticky="w", pady=4)
        ttk.Entry(create_frame, textvariable=self.new_category_var, width=28).grid(row=0, column=1, sticky="w", pady=4, padx=(6, 12))
        ttk.Button(create_frame, text="Создать категорию", command=self.create_category).grid(row=0, column=2, sticky="w")

        ttk.Label(create_frame, text="Новая подкатегория").grid(row=1, column=0, sticky="w", pady=4)
        ttk.Entry(create_frame, textvariable=self.new_subcategory_var, width=28).grid(row=1, column=1, sticky="w", pady=4, padx=(6, 12))
        ttk.Label(create_frame, text="Категория").grid(row=1, column=2, sticky="w", padx=(10, 6))
        self.subcategory_parent_box = ttk.Combobox(create_frame, textvariable=self.subcategory_parent_var, state="readonly", width=28)
        self.subcategory_parent_box.grid(row=1, column=3, sticky="w", pady=4)
        ttk.Button(create_frame, text="Создать подкатегорию", command=self.create_subcategory).grid(row=1, column=4, sticky="w", padx=(10, 0))

        trees = ttk.Frame(self.categories_tab, style="Panel.TFrame")
        trees.pack(fill="both", expand=True)

        self.categories_tree = ttk.Treeview(trees, columns=("id", "name"), show="headings", height=10)
        self.categories_tree.pack(side="left", fill="both", expand=True, padx=(0, 8))
        self.categories_tree.heading("id", text="ID")
        self.categories_tree.heading("name", text="Категория")
        self.categories_tree.column("id", width=70, anchor="w")
        self.categories_tree.column("name", width=260, anchor="w")

        self.subcategories_tree = ttk.Treeview(trees, columns=("id", "name", "category"), show="headings", height=10)
        self.subcategories_tree.pack(side="left", fill="both", expand=True)
        self.subcategories_tree.heading("id", text="ID")
        self.subcategories_tree.heading("name", text="Подкатегория")
        self.subcategories_tree.heading("category", text="Категория")
        self.subcategories_tree.column("id", width=70, anchor="w")
        self.subcategories_tree.column("name", width=220, anchor="w")
        self.subcategories_tree.column("category", width=220, anchor="w")

    def create_category(self):
        name = self.new_category_var.get().strip()
        if not name:
            messagebox.showwarning("Валидация", "Введите название категории")
            return
        try:
            self.db.execute("INSERT INTO categories(name) VALUES(?)", (name,))
        except sqlite3.IntegrityError:
            messagebox.showerror("Ошибка", "Категория уже существует")
            return
        self.new_category_var.set("")
        self.refresh_all()

    def create_subcategory(self):
        name = self.new_subcategory_var.get().strip()
        cat_token = self.subcategory_parent_var.get().strip()
        if not name or not cat_token:
            messagebox.showwarning("Валидация", "Укажите подкатегорию и категорию")
            return
        category_id = int(cat_token.split(" | ")[0])
        try:
            self.db.execute("INSERT INTO subcategories(category_id, name) VALUES(?, ?)", (category_id, name))
        except sqlite3.IntegrityError:
            messagebox.showerror("Ошибка", "Подкатегория уже существует в выбранной категории")
            return
        self.new_subcategory_var.set("")
        self.refresh_all()

    def _build_nomenclature_tab(self):
        controls = ttk.Frame(self.nomenclature_tab, style="Panel.TFrame")
        controls.pack(fill="x", pady=(0, 10))

        ttk.Label(controls, text="Поиск по марке").pack(side="left")
        ttk.Entry(controls, textvariable=self.nomenclature_brand_filter, width=28).pack(side="left", padx=(8, 14))

        ttk.Button(controls, text="Создать карточку товара", command=self.open_create_product_dialog).pack(side="left")
        ttk.Button(controls, text="Редактировать карточку", command=self.open_edit_product_dialog).pack(side="left", padx=(10, 0))
        ttk.Button(controls, text="Удалить карточку", command=self.delete_selected_product).pack(side="left", padx=(10, 0))

        columns = (
            "id",
            "brand",
            "supplier",
            "client",
            "unit",
            "volume",
            "weight",
            "barcode",
            "serial_tracking",
            "category",
            "subcategory",
            "product_owner",
        )
        self.nomenclature_tree = ttk.Treeview(self.nomenclature_tab, columns=columns, show="headings")
        self.nomenclature_tree.pack(fill="both", expand=True)

        headings = [
            ("id", "ID", 60),
            ("brand", "Марка", 180),
            ("supplier", "Поставщик", 150),
            ("client", "3PL клиент", 150),
            ("unit", "Ед. изм.", 100),
            ("volume", "Объём", 90),
            ("weight", "Вес", 90),
            ("barcode", "Штрих-код", 140),
            ("serial_tracking", "Серийный учёт", 120),
            ("category", "Категория товара", 150),
            ("subcategory", "Подкатегория товара", 170),
            ("product_owner", "Продакт", 140),
        ]
        for col, title, width in headings:
            self.nomenclature_tree.heading(col, text=title)
            self.nomenclature_tree.column(col, width=width, anchor="w")

        self.copy_menu = tk.Menu(self, tearoff=0)
        self.copy_menu.add_command(label="Скопировать", command=self.copy_selected_value)
        self.nomenclature_tree.bind("<Button-3>", self.show_copy_menu)

    def _build_inbound_tab(self):
        controls = ttk.Frame(self.inbound_tab, style="Panel.TFrame")
        controls.pack(fill="x", pady=(0, 10))

        ttk.Label(controls, text="Поиск по номеру заказа").pack(side="left")
        ttk.Entry(controls, textvariable=self.inbound_order_search_var, width=20).pack(side="left", padx=(8, 10))

        ttk.Label(controls, text="Фильтр по статусу").pack(side="left")
        status_box = ttk.Combobox(
            controls,
            textvariable=self.inbound_status_var,
            values=["Все", "Новый", "Принят"],
            state="readonly",
            width=12,
        )
        status_box.pack(side="left", padx=(8, 10))
        status_box.bind("<<ComboboxSelected>>", lambda *_: self.refresh_inbound_orders())

        ttk.Label(controls, text="Фильтр по дате создания").pack(side="left")
        ttk.Entry(controls, textvariable=self.inbound_date_filter_var, width=14).pack(side="left", padx=(8, 10))
        ttk.Button(controls, text="Применить", command=self.refresh_inbound_orders).pack(side="left")
        ttk.Button(controls, text="Создать Новый заказ", command=self.open_create_inbound_order_dialog).pack(side="right")

        columns = (
            "order_number",
            "created_at",
            "received_at",
            "created_by",
            "supplier",
            "client",
            "warehouse",
            "status",
            "planned_qty",
            "actual_qty",
        )
        self.inbound_tree = ttk.Treeview(self.inbound_tab, columns=columns, show="headings")
        self.inbound_tree.pack(fill="both", expand=True)
        self.inbound_tree.bind("<Double-1>", self.open_selected_inbound_order)

        headings = [
            ("order_number", "Номер заказа", 130),
            ("created_at", "Дата создания", 150),
            ("received_at", "Дата приёма", 140),
            ("created_by", "Создал", 110),
            ("supplier", "Поставщик", 150),
            ("client", "3PL клиент", 140),
            ("warehouse", "Склад", 140),
            ("status", "Статус", 90),
            ("planned_qty", "Плановое количество", 150),
            ("actual_qty", "Фактическое количество", 170),
        ]
        for col, title, width in headings:
            self.inbound_tree.heading(col, text=title)
            self.inbound_tree.column(col, width=width, anchor="w")

    def open_selected_inbound_order(self, _event=None):
        selected = self.inbound_tree.selection()
        if not selected:
            return
        order_number = self.inbound_tree.item(selected[0], "values")[0]
        self.open_inbound_order_dialog(order_number)

    def open_inbound_order_dialog(self, order_number: str):
        order_rows = self.db.query(
            """
            SELECT o.id, o.order_number, o.created_at, COALESCE(o.received_at, ''),
                   s.name, c.name, w.name, o.status, o.created_by
            FROM inbound_orders o
            JOIN suppliers s ON s.id = o.supplier_id
            JOIN clients c ON c.id = o.client_id
            JOIN warehouses w ON w.id = o.warehouse_id
            WHERE o.order_number = ?
            """,
            (order_number,),
        )
        if not order_rows:
            messagebox.showerror("Ошибка", "Заказ не найден")
            return

        order_id, order_no, created_at, received_at, supplier_name, client_name, warehouse_name, status, created_by = order_rows[0]

        dialog = tk.Toplevel(self)
        dialog.title(f"Приёмка заказа {order_no}")
        dialog.geometry("1320x780")
        dialog.transient(self)
        dialog.grab_set()

        frame = ttk.Frame(dialog, style="Panel.TFrame", padding=14)
        frame.pack(fill="both", expand=True)

        header = [
            ("Номер заказа", order_no),
            ("Дата создания", created_at),
            ("Поставщик", supplier_name),
            ("3PL клиент", client_name),
            ("Склад", warehouse_name),
            ("Статус", status),
            ("Создал", created_by),
            ("Дата приёма", received_at),
        ]
        for i, (k, v) in enumerate(header):
            r = i // 4
            c = (i % 4) * 2
            ttk.Label(frame, text=k).grid(row=r, column=c, sticky="w", padx=(0, 6), pady=4)
            entry = ttk.Entry(frame, width=30)
            entry.grid(row=r, column=c + 1, sticky="w", pady=4)
            entry.insert(0, v)
            entry.configure(state="readonly")

        toolbar = ttk.Frame(frame, style="Panel.TFrame")
        toolbar.grid(row=2, column=0, columnspan=8, sticky="ew", pady=(6, 6))

        columns = (
            "line_id",
            "product_name",
            "category",
            "subcategory",
            "unit",
            "planned_qty",
            "actual_qty",
            "weight",
            "volume",
            "barcode",
            "serial_count",
            "serial_tracking",
        )
        lines_tree = ttk.Treeview(frame, columns=columns, show="headings", height=16)
        lines_tree.grid(row=3, column=0, columnspan=8, sticky="nsew", pady=(0, 8))
        frame.grid_rowconfigure(3, weight=1)
        for col, title, width in [
            ("line_id", "ID", 50),
            ("product_name", "Название товара", 180),
            ("category", "Категория", 130),
            ("subcategory", "Подкатегория", 140),
            ("unit", "Ед.", 70),
            ("planned_qty", "Плановое кол-во", 130),
            ("actual_qty", "Фактическое кол-во", 140),
            ("weight", "Вес", 100),
            ("volume", "Объём", 100),
            ("barcode", "Штрихкод", 140),
            ("serial_count", "Серийный номер", 150),
            ("serial_tracking", "Серийный учёт", 120),
        ]:
            lines_tree.heading(col, text=title)
            lines_tree.column(col, width=width, anchor="w")

        lines_tree.tag_configure("qty_less", background="#FFF4D6")
        lines_tree.tag_configure("qty_more", background="#FDE2E1")

        def serial_count_text(serials: str):
            serial_list = [x.strip() for x in (serials or "").split(",") if x.strip()]
            return str(len(serial_list))

        def qty_tag(planned: float, actual: float):
            if actual < planned:
                return "qty_less"
            if actual > planned:
                return "qty_more"
            return ""

        def load_lines():
            for item in lines_tree.get_children():
                lines_tree.delete(item)
            rows = self.db.query(
                """
                SELECT i.id,
                       COALESCE(p.name, p.brand, ''),
                       COALESCE(cat.name, ''),
                       COALESCE(sub.name, ''),
                       COALESCE(p.unit, ''),
                       i.planned_qty,
                       i.actual_qty,
                       COALESCE(p.weight, 0) * i.actual_qty,
                       COALESCE(p.volume, 0) * i.actual_qty,
                       COALESCE(p.barcode, ''),
                       COALESCE(i.serial_numbers, ''),
                       COALESCE(p.serial_tracking, 'Нет')
                FROM inbound_order_items i
                JOIN products p ON p.id = i.product_id
                LEFT JOIN categories cat ON cat.id = i.category_id
                LEFT JOIN subcategories sub ON sub.id = i.subcategory_id
                WHERE i.order_id = ?
                ORDER BY i.id
                """,
                (order_id,),
            )
            for rid, pname, cat, sub, unit, planned, actual, w, v, barcode, serials, serial_tracking in rows:
                tag = qty_tag(float(planned), float(actual))
                lines_tree.insert(
                    "",
                    "end",
                    values=(rid, pname, cat, sub, unit, planned, actual, w, v, barcode, serial_count_text(serials), serial_tracking),
                    tags=(tag,) if tag else (),
                )

        def set_actual_qty(line_id: int, qty: float, serial_numbers: str | None = None):
            if serial_numbers is None:
                self.db.execute("UPDATE inbound_order_items SET actual_qty = ?, actual_filled = 1 WHERE id = ?", (qty, line_id))
            else:
                self.db.execute(
                    "UPDATE inbound_order_items SET actual_qty = ?, actual_filled = 1, serial_numbers = ? WHERE id = ?",
                    (qty, serial_numbers, line_id),
                )

        def edit_serial_line(line_id: int, product_name: str, current_serial: str):
            serial_dialog = tk.Toplevel(dialog)
            serial_dialog.title(f"Сканирование серий: {product_name}")
            serial_dialog.geometry("620x520")
            serial_dialog.transient(dialog)
            serial_dialog.grab_set()

            serial_frame = ttk.Frame(serial_dialog, style="Panel.TFrame", padding=12)
            serial_frame.pack(fill="both", expand=True)

            ttk.Label(serial_frame, text="Серия").pack(anchor="w")
            serial_input = tk.StringVar()
            serial_entry = ttk.Entry(serial_frame, textvariable=serial_input, width=40)
            serial_entry.pack(anchor="w", pady=(2, 8))
            serial_entry.focus_set()

            serials = [x.strip() for x in (current_serial or "").split(",") if x.strip()]
            listbox = tk.Listbox(serial_frame, height=16)
            listbox.pack(fill="both", expand=True)
            for ser in serials:
                listbox.insert("end", ser)

            count_var = tk.StringVar(value=f"Отсканировано: {len(serials)}")
            ttk.Label(serial_frame, textvariable=count_var).pack(anchor="w", pady=(8, 0))

            def refresh_count():
                count_var.set(f"Отсканировано: {listbox.size()}")

            def add_serial(_event=None):
                val = serial_input.get().strip()
                if not val:
                    return
                existing = [listbox.get(i) for i in range(listbox.size())]
                if val in existing:
                    messagebox.showwarning("Валидация", "Эта серия уже добавлена", parent=serial_dialog)
                    return
                listbox.insert("end", val)
                serial_input.set("")
                refresh_count()

            def remove_selected():
                sel = listbox.curselection()
                if not sel:
                    return
                listbox.delete(sel[0])
                refresh_count()

            def finish_scan():
                out = [listbox.get(i) for i in range(listbox.size())]
                set_actual_qty(line_id, float(len(out)), ", ".join(out))
                serial_dialog.destroy()
                load_lines()
                self.refresh_inbound_orders()

            btns = ttk.Frame(serial_frame, style="Panel.TFrame")
            btns.pack(fill="x", pady=(8, 0))
            ttk.Button(btns, text="Добавить серию", command=add_serial).pack(side="left")
            ttk.Button(btns, text="Удалить выбранную", command=remove_selected).pack(side="left", padx=(8, 0))
            ttk.Button(btns, text="Завершить", command=finish_scan).pack(side="right")

            serial_entry.bind("<Return>", add_serial)

        def edit_selected_line():
            sel = lines_tree.selection()
            if not sel:
                messagebox.showwarning("Валидация", "Выберите позицию", parent=dialog)
                return
            values = lines_tree.item(sel[0], "values")
            line_id = int(values[0])
            product_name = values[1]

            row = self.db.query(
                """
                SELECT i.actual_qty, COALESCE(i.serial_numbers, ''), COALESCE(p.serial_tracking, 'Нет')
                FROM inbound_order_items i
                JOIN products p ON p.id = i.product_id
                WHERE i.id = ?
                """,
                (line_id,),
            )[0]
            current_actual, current_serial, serial_tracking_db = float(row[0]), row[1], row[2]

            if serial_tracking_db == "Да":
                edit_serial_line(line_id, product_name, current_serial)
                return

            qty_text = simpledialog.askstring(
                "Фактическое количество",
                f"Введите количество, которое принимаете сейчас для товара: {product_name}",
                initialvalue="1",
                parent=dialog,
            )
            if qty_text is None:
                return
            try:
                delta_qty = float(qty_text)
                if delta_qty <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showwarning("Валидация", "Количество должно быть положительным числом", parent=dialog)
                return

            set_actual_qty(line_id, current_actual + delta_qty)
            load_lines()
            self.refresh_inbound_orders()

        def edit_selected_qty():
            sel = lines_tree.selection()
            if not sel:
                messagebox.showwarning("Валидация", "Выберите позицию", parent=dialog)
                return
            values = lines_tree.item(sel[0], "values")
            line_id = int(values[0])
            product_name = values[1]

            row = self.db.query(
                """
                SELECT i.actual_qty, COALESCE(i.serial_numbers, ''), COALESCE(p.serial_tracking, 'Нет')
                FROM inbound_order_items i
                JOIN products p ON p.id = i.product_id
                WHERE i.id = ?
                """,
                (line_id,),
            )[0]
            current_actual, current_serial, serial_tracking_db = float(row[0]), row[1], row[2]

            qty_text = simpledialog.askstring(
                "Редактировать количество",
                f"Введите новое фактическое количество для товара: {product_name}",
                initialvalue=str(current_actual),
                parent=dialog,
            )
            if qty_text is None:
                return
            try:
                new_qty = float(qty_text)
                if new_qty < 0:
                    raise ValueError
            except ValueError:
                messagebox.showwarning("Валидация", "Количество должно быть числом >= 0", parent=dialog)
                return

            if serial_tracking_db == "Да":
                if abs(new_qty - int(new_qty)) > 1e-9:
                    messagebox.showwarning("Валидация", "Для серийного товара количество должно быть целым", parent=dialog)
                    return
                serial_list = [x.strip() for x in (current_serial or "").split(",") if x.strip()]
                need = int(new_qty)
                if need < len(serial_list):
                    if not messagebox.askyesno(
                        "Подтверждение",
                        "Новое количество меньше числа отсканированных серий. Обрезать список серий?",
                        parent=dialog,
                    ):
                        return
                    serial_list = serial_list[:need]
                set_actual_qty(line_id, float(need), ", ".join(serial_list))
            else:
                set_actual_qty(line_id, new_qty)

            load_lines()
            self.refresh_inbound_orders()

        def accept_order():
            state = self.db.query("SELECT status FROM inbound_orders WHERE id = ?", (order_id,))[0][0]
            if state == "Принят":
                messagebox.showinfo("Информация", "Заказ уже принят", parent=dialog)
                return

            check_rows = self.db.query(
                """
                SELECT i.id, i.planned_qty, i.actual_qty, i.actual_filled, i.product_id,
                       COALESCE(i.serial_numbers, ''), COALESCE(p.serial_tracking, 'Нет')
                FROM inbound_order_items i
                JOIN products p ON p.id = i.product_id
                WHERE i.order_id = ?
                """,
                (order_id,),
            )
            if not check_rows:
                messagebox.showwarning("Валидация", "В заказе нет позиций", parent=dialog)
                return

            not_filled = [r for r in check_rows if int(r[3]) != 1]
            if not_filled:
                messagebox.showwarning("Валидация", "Заполните фактическое количество для всех позиций", parent=dialog)
                return

            for _, planned_qty, actual_qty, _, _, serial_numbers, serial_tracking in check_rows:
                planned = float(planned_qty)
                actual = float(actual_qty)
                if actual > planned:
                    if not messagebox.askyesno("Подтверждение", "Зафиксировать излишек?", parent=dialog):
                        return
                elif actual < planned:
                    if not messagebox.askyesno("Подтверждение", "Зафиксировать недостачу?", parent=dialog):
                        return

                if serial_tracking == "Да":
                    serial_count = len([x for x in (serial_numbers or "").split(",") if x.strip()])
                    if int(actual) != serial_count:
                        messagebox.showwarning(
                            "Валидация",
                            "Количество серийных номеров должно быть равно фактическому количеству для серийного товара.",
                            parent=dialog,
                        )
                        return

            for _, _, actual_qty, _, product_id, _, _ in check_rows:
                qty = float(actual_qty)
                if qty > 0:
                    if abs(qty - int(qty)) > 1e-9:
                        messagebox.showwarning(
                            "Валидация",
                            "Для проведения в остаток фактическое количество должно быть целым числом.",
                            parent=dialog,
                        )
                        return
                    self.db.execute(
                        "INSERT INTO movements(product_id, movement_type, quantity, reference, moved_at, note) VALUES(?, 'IN', ?, ?, ?, ?)",
                        (
                            product_id,
                            int(qty),
                            order_no,
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "Приём товара по заказу",
                        ),
                    )

            self.db.execute(
                "UPDATE inbound_orders SET status = 'Принят', received_at = ? WHERE id = ?",
                (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), order_id),
            )
            messagebox.showinfo("Готово", "Заказ принят", parent=dialog)
            self.refresh_all()
            dialog.destroy()

        fullscreen_state = {"on": False}

        def toggle_fullscreen():
            try:
                if not fullscreen_state["on"]:
                    dialog.state("zoomed")
                    fullscreen_state["on"] = True
                    fullscreen_btn.configure(text="Обычный размер")
                else:
                    dialog.state("normal")
                    fullscreen_state["on"] = False
                    fullscreen_btn.configure(text="Во весь экран")
            except tk.TclError:
                current = bool(dialog.attributes("-fullscreen"))
                dialog.attributes("-fullscreen", not current)
                fullscreen_state["on"] = not current
                fullscreen_btn.configure(text="Обычный размер" if fullscreen_state["on"] else "Во весь экран")

        dialog.bind("<Escape>", lambda _e: dialog.attributes("-fullscreen", False))

        ttk.Button(toolbar, text="Ввести фактическое", command=edit_selected_line).pack(side="left")
        ttk.Button(toolbar, text="Редактировать кол-во", command=edit_selected_qty).pack(side="left", padx=(8, 0))
        ttk.Button(toolbar, text="Принять заказ", command=accept_order).pack(side="left", padx=(8, 0))
        fullscreen_btn = ttk.Button(toolbar, text="Во весь экран", command=toggle_fullscreen)
        fullscreen_btn.pack(side="right")

        load_lines()

    def _next_inbound_order_number(self):
        row = self.db.query("SELECT order_number FROM inbound_orders ORDER BY id DESC LIMIT 1")
        if not row or not row[0][0]:
            return "IN-00001"
        val = row[0][0]
        try:
            nxt = int(val.split("-")[-1]) + 1
        except ValueError:
            nxt = self.db.query("SELECT COUNT(*) FROM inbound_orders")[0][0] + 1
        return f"IN-{nxt:05d}"

    def open_create_inbound_order_dialog(self):
        dialog = tk.Toplevel(self)
        dialog.title("Создание заказа на приход")
        dialog.geometry("1180x720")
        dialog.transient(self)
        dialog.grab_set()

        frame = ttk.Frame(dialog, style="Panel.TFrame", padding=14)
        frame.pack(fill="both", expand=True)

        order_number_var = tk.StringVar(value=self._next_inbound_order_number())
        created_at_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        status_var = tk.StringVar(value="Новый")
        created_by_var = tk.StringVar(value=self.current_user)
        supplier_var = tk.StringVar()
        client_var = tk.StringVar()
        warehouse_var = tk.StringVar()

        suppliers = self._load_reference_dict("suppliers")
        clients = self._load_reference_dict("clients")
        warehouses = self._load_reference_dict("warehouses")

        header_fields = [
            ("Номер заказа", order_number_var, True),
            ("Дата создания заказа", created_at_var, True),
            ("Статус", status_var, True),
            ("Создал", created_by_var, True),
        ]
        for i, (label, var, readonly) in enumerate(header_fields):
            r = i // 2
            c = (i % 2) * 2
            ttk.Label(frame, text=label).grid(row=r, column=c, sticky="w", pady=4, padx=(0, 6))
            ttk.Entry(frame, textvariable=var, state="readonly" if readonly else "normal", width=30).grid(row=r, column=c + 1, sticky="w", pady=4)

        ttk.Label(frame, text="Поставщик").grid(row=2, column=0, sticky="w", pady=4)
        ttk.Combobox(frame, textvariable=supplier_var, values=list(suppliers.keys()), state="readonly", width=33).grid(row=2, column=1, sticky="w", pady=4)
        ttk.Label(frame, text="3PL клиент").grid(row=2, column=2, sticky="w", pady=4)
        ttk.Combobox(frame, textvariable=client_var, values=list(clients.keys()), state="readonly", width=33).grid(row=2, column=3, sticky="w", pady=4)

        ttk.Label(frame, text="Склад").grid(row=3, column=0, sticky="w", pady=4)
        ttk.Combobox(frame, textvariable=warehouse_var, values=list(warehouses.keys()), state="readonly", width=33).grid(row=3, column=1, sticky="w", pady=4)

        ttk.Separator(frame, orient="horizontal").grid(row=4, column=0, columnspan=4, sticky="ew", pady=10)
        ttk.Label(frame, text="Структура заказа", style="Section.TLabel").grid(row=5, column=0, columnspan=2, sticky="w", pady=(0, 8))

        line_category_var = tk.StringVar()
        line_subcategory_var = tk.StringVar()
        line_product_var = tk.StringVar()
        line_qty_var = tk.StringVar(value="1")
        line_weight_var = tk.StringVar(value="0")
        line_volume_var = tk.StringVar(value="0")
        line_unit_var = tk.StringVar(value="")

        categories = self._load_reference_dict("categories")

        ttk.Label(frame, text="Категория").grid(row=6, column=0, sticky="w", pady=4)
        line_category_box = ttk.Combobox(frame, textvariable=line_category_var, values=list(categories.keys()), state="readonly", width=33)
        line_category_box.grid(row=6, column=1, sticky="w", pady=4)

        ttk.Label(frame, text="Подкатегория").grid(row=6, column=2, sticky="w", pady=4)
        line_subcategory_box = ttk.Combobox(frame, textvariable=line_subcategory_var, state="readonly", width=33)
        line_subcategory_box.grid(row=6, column=3, sticky="w", pady=4)

        ttk.Label(frame, text="Товар").grid(row=7, column=0, sticky="w", pady=4)
        line_product_box = ttk.Combobox(frame, textvariable=line_product_var, state="readonly", width=33)
        line_product_box.grid(row=7, column=1, sticky="w", pady=4)

        ttk.Label(frame, text="Плановое кол-во").grid(row=7, column=2, sticky="w", pady=4)
        qty_row = ttk.Frame(frame, style="Panel.TFrame")
        qty_row.grid(row=7, column=3, sticky="w", pady=4)
        ttk.Entry(qty_row, textvariable=line_qty_var, width=12).pack(side="left")
        ttk.Label(qty_row, textvariable=line_unit_var).pack(side="left", padx=(8, 0))

        ttk.Label(frame, text="Вес").grid(row=8, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=line_weight_var, state="readonly", width=35).grid(row=8, column=1, sticky="w", pady=4)

        ttk.Label(frame, text="Объём").grid(row=8, column=2, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=line_volume_var, state="readonly", width=35).grid(row=8, column=3, sticky="w", pady=4)

        order_items = []
        product_catalog = {}

        lines_tree = ttk.Treeview(
            frame,
            columns=("category", "subcategory", "product", "unit", "planned_qty", "weight", "volume"),
            show="headings",
            height=8,
        )
        lines_tree.grid(row=10, column=0, columnspan=4, sticky="nsew", pady=(8, 6))
        frame.grid_rowconfigure(10, weight=1)
        for col, title, width in [
            ("category", "Категория", 150),
            ("subcategory", "Подкатегория", 170),
            ("product", "Товар", 220),
            ("unit", "Ед.", 80),
            ("planned_qty", "Плановое кол-во", 140),
            ("weight", "Вес", 100),
            ("volume", "Объём", 100),
        ]:
            lines_tree.heading(col, text=title)
            lines_tree.column(col, width=width, anchor="w")

        def refresh_products_for_subcategory(*_):
            sub_token = line_subcategory_var.get().strip()
            if not sub_token or "|" not in sub_token:
                line_product_box["values"] = []
                return
            sub_id = int(sub_token.split(" | ")[0])
            rows = self.db.query(
                """
                SELECT id, brand, unit, COALESCE(weight, 0), COALESCE(volume, 0)
                FROM products
                WHERE subcategory_id = ?
                ORDER BY brand
                """,
                (sub_id,),
            )
            product_catalog.clear()
            values = []
            for pid, brand, unit, weight, volume in rows:
                token = f"{pid} | {brand or ''}"
                values.append(token)
                product_catalog[token] = {"id": pid, "brand": brand or "", "unit": unit or "Шт", "weight": float(weight or 0), "volume": float(volume or 0)}
            line_product_box["values"] = values
            if values:
                line_product_var.set(values[0])
                update_calculated_fields()

        def refresh_subcategories_for_category(*_):
            cat_token = line_category_var.get().strip()
            if not cat_token or "|" not in cat_token:
                line_subcategory_box["values"] = []
                line_subcategory_var.set("")
                line_product_box["values"] = []
                line_product_var.set("")
                return
            cat_id = int(cat_token.split(" | ")[0])
            rows = self.db.query("SELECT id, name FROM subcategories WHERE category_id = ? ORDER BY name", (cat_id,))
            values = [f"{sid} | {name}" for sid, name in rows]
            line_subcategory_box["values"] = values
            line_subcategory_var.set(values[0] if values else "")
            refresh_products_for_subcategory()

        def update_calculated_fields(*_):
            token = line_product_var.get().strip()
            info = product_catalog.get(token)
            if not info:
                line_unit_var.set("")
                line_weight_var.set("0")
                line_volume_var.set("0")
                return
            line_unit_var.set(info["unit"])
            try:
                qty = float(line_qty_var.get())
            except ValueError:
                qty = 0
            line_weight_var.set(f"{qty * info['weight']:.3f}")
            line_volume_var.set(f"{qty * info['volume']:.3f}")

        line_category_var.trace_add("write", refresh_subcategories_for_category)
        line_subcategory_var.trace_add("write", refresh_products_for_subcategory)
        line_product_var.trace_add("write", update_calculated_fields)
        line_qty_var.trace_add("write", update_calculated_fields)

        def add_line():
            cat = line_category_var.get().strip()
            sub = line_subcategory_var.get().strip()
            prod = line_product_var.get().strip()
            if not cat or not sub or not prod:
                messagebox.showwarning("Валидация", "Выберите категорию, подкатегорию и товар", parent=dialog)
                return
            try:
                qty = float(line_qty_var.get())
                if qty <= 0:
                    raise ValueError
            except ValueError:
                messagebox.showwarning("Валидация", "Плановое количество должно быть положительным числом", parent=dialog)
                return

            info = product_catalog.get(prod)
            if not info:
                messagebox.showwarning("Валидация", "Выберите корректный товар", parent=dialog)
                return

            cat_id = int(cat.split(" | ")[0])
            sub_id = int(sub.split(" | ")[0])
            planned_weight = qty * info["weight"]
            planned_volume = qty * info["volume"]
            item = {
                "category_id": cat_id,
                "subcategory_id": sub_id,
                "product_id": info["id"],
                "category_name": cat.split(" | ", 1)[1],
                "subcategory_name": sub.split(" | ", 1)[1],
                "brand": info["brand"],
                "unit": info["unit"],
                "planned_qty": qty,
                "planned_weight": planned_weight,
                "planned_volume": planned_volume,
            }
            order_items.append(item)
            lines_tree.insert(
                "",
                "end",
                values=(
                    item["category_name"],
                    item["subcategory_name"],
                    item["brand"],
                    item["unit"],
                    f"{qty:.3f}",
                    f"{planned_weight:.3f}",
                    f"{planned_volume:.3f}",
                ),
            )

        ttk.Button(frame, text="Добавить позицию", command=add_line).grid(row=9, column=3, sticky="e", pady=(6, 0))

        def read_id(token):
            if token and "|" in token:
                return int(token.split(" | ")[0])
            return None

        def save_order():
            supplier_id = read_id(supplier_var.get())
            client_id = read_id(client_var.get())
            warehouse_id = read_id(warehouse_var.get())
            if not supplier_id or not client_id or not warehouse_id:
                messagebox.showwarning("Валидация", "Укажите поставщика, 3PL клиента и склад", parent=dialog)
                return
            if not order_items:
                messagebox.showwarning("Валидация", "Добавьте хотя бы одну позицию в заказ", parent=dialog)
                return

            try:
                order_id = self.db.execute(
                    """
                    INSERT INTO inbound_orders(order_number, created_at, received_at, created_by, supplier_id, client_id, warehouse_id, status)
                    VALUES(?, ?, NULL, ?, ?, ?, ?, ?)
                    """,
                    (
                        order_number_var.get().strip(),
                        created_at_var.get().strip(),
                        created_by_var.get().strip(),
                        supplier_id,
                        client_id,
                        warehouse_id,
                        "Новый",
                    ),
                )
                for item in order_items:
                    self.db.execute(
                        """
                        INSERT INTO inbound_order_items(
                            order_id, category_id, subcategory_id, product_id,
                            planned_qty, actual_qty, actual_filled, planned_weight, planned_volume, serial_numbers
                        ) VALUES(?, ?, ?, ?, ?, 0, 0, ?, ?, '')
                        """,
                        (
                            order_id,
                            item["category_id"],
                            item["subcategory_id"],
                            item["product_id"],
                            item["planned_qty"],
                            item["planned_weight"],
                            item["planned_volume"],
                        ),
                    )
            except sqlite3.IntegrityError as exc:
                messagebox.showerror("Ошибка", f"Не удалось сохранить заказ: {exc}", parent=dialog)
                return

            self.refresh_all()
            dialog.destroy()

        buttons = ttk.Frame(frame, style="Panel.TFrame")
        buttons.grid(row=11, column=0, columnspan=4, sticky="e", pady=(8, 0))
        ttk.Button(buttons, text="Сохранить заказ", command=save_order).pack(side="right")

    def _build_movements_tab(self):
        form = ttk.Frame(self.movements_tab, style="Panel.TFrame")
        form.pack(fill="x", pady=(0, 12))

        ttk.Label(form, text="Товар").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=4)
        ttk.Label(form, text="Тип").grid(row=0, column=2, sticky="w", padx=(0, 6), pady=4)
        ttk.Label(form, text="Кол-во").grid(row=0, column=4, sticky="w", padx=(0, 6), pady=4)
        ttk.Label(form, text="Документ").grid(row=0, column=6, sticky="w", padx=(0, 6), pady=4)
        ttk.Label(form, text="Комментарий").grid(row=0, column=8, sticky="w", padx=(0, 6), pady=4)

        self.movement_product = tk.StringVar()
        self.movement_type = tk.StringVar(value="IN")
        self.movement_qty = tk.StringVar(value="1")
        self.movement_ref = tk.StringVar()
        self.movement_note = tk.StringVar()

        self.movement_product_box = ttk.Combobox(form, textvariable=self.movement_product, width=28, state="readonly")
        self.movement_product_box.grid(row=0, column=1, padx=(0, 12), pady=4)
        ttk.Combobox(form, textvariable=self.movement_type, values=["IN", "OUT"], width=8, state="readonly").grid(row=0, column=3, padx=(0, 12), pady=4)
        ttk.Entry(form, textvariable=self.movement_qty, width=10).grid(row=0, column=5, padx=(0, 12), pady=4)
        ttk.Entry(form, textvariable=self.movement_ref, width=16).grid(row=0, column=7, padx=(0, 12), pady=4)
        ttk.Entry(form, textvariable=self.movement_note, width=26).grid(row=0, column=9, padx=(0, 12), pady=4)
        ttk.Button(form, text="Провести", command=self.add_movement).grid(row=0, column=10)

        self.movements_tree = ttk.Treeview(self.movements_tab, columns=("id", "brand", "type", "qty", "reference", "date", "note"), show="headings")
        self.movements_tree.pack(fill="both", expand=True)
        for col, title, width in [("id", "ID", 60), ("brand", "Марка", 220), ("type", "Тип", 80), ("qty", "Кол-во", 90), ("reference", "Документ", 140), ("date", "Дата", 180), ("note", "Комментарий", 260)]:
            self.movements_tree.heading(col, text=title)
            self.movements_tree.column(col, width=width, anchor="w")

    def _build_stock_tab(self):
        top = ttk.Frame(self.stock_tab, style="Panel.TFrame")
        top.pack(fill="x", pady=(0, 10))
        ttk.Label(top, text="Онлайн остатки по номенклатуре", style="Section.TLabel").pack(side="left")
        ttk.Button(top, text="Обновить", command=self.refresh_stock).pack(side="right")

        self.stock_tree = ttk.Treeview(self.stock_tab, columns=("brand", "client", "unit", "stock"), show="headings")
        self.stock_tree.pack(fill="both", expand=True)
        for col, title, width in [("brand", "Марка", 280), ("client", "3PL клиент", 220), ("unit", "Ед.", 80), ("stock", "Остаток", 120)]:
            self.stock_tree.heading(col, text=title)
            self.stock_tree.column(col, width=width, anchor="w")

    def _clear_tree(self, tree):
        for row in tree.get_children():
            tree.delete(row)

    def _load_reference_dict(self, table_name):
        rows = self.db.query(f"SELECT id, name FROM {table_name} ORDER BY name")
        return {f"{r[0]} | {r[1]}": r[0] for r in rows}

    def open_create_product_dialog(self):
        self._open_product_dialog(mode="create")

    def open_edit_product_dialog(self):
        selected = self.nomenclature_tree.selection()
        if not selected:
            messagebox.showwarning("Валидация", "Выберите карточку для редактирования")
            return
        self._open_product_dialog(mode="edit", product_id=int(self.nomenclature_tree.item(selected[0], "values")[0]))

    def _open_product_dialog(self, mode: str, product_id: int | None = None):
        dialog = tk.Toplevel(self)
        dialog.title("Карточка товара")
        dialog.geometry("560x580")
        dialog.transient(self)
        dialog.grab_set()

        frm = ttk.Frame(dialog, style="Panel.TFrame", padding=14)
        frm.pack(fill="both", expand=True)

        brand_var = tk.StringVar()
        supplier_var = tk.StringVar()
        client_var = tk.StringVar()
        unit_var = tk.StringVar(value="Шт")
        volume_var = tk.StringVar()
        weight_var = tk.StringVar()
        barcode_var = tk.StringVar()
        serial_var = tk.StringVar(value="Нет")
        category_var = tk.StringVar()
        subcategory_var = tk.StringVar()
        product_owner_var = tk.StringVar(value=self.current_user)

        suppliers = self._load_reference_dict("suppliers")
        clients = self._load_reference_dict("clients")
        categories = self._load_reference_dict("categories")
        subcategories = self._load_reference_dict("subcategories")

        def load_all_subcategories():
            rows = self.db.query("SELECT id, name FROM subcategories ORDER BY name")
            options = [f"{r[0]} | {r[1]}" for r in rows]
            subcategory_box["values"] = options
            if options and not subcategory_var.get():
                subcategory_var.set(options[0])

        fields = [
            ("Марка", ttk.Entry(frm, textvariable=brand_var, width=36)),
            ("Поставщик", ttk.Combobox(frm, textvariable=supplier_var, values=list(suppliers.keys()), state="readonly", width=33)),
            ("3PL клиент", ttk.Combobox(frm, textvariable=client_var, values=list(clients.keys()), state="readonly", width=33)),
            ("Единица измерения", ttk.Combobox(frm, textvariable=unit_var, values=["Шт", "Палета"], state="readonly", width=33)),
            ("Объём", ttk.Entry(frm, textvariable=volume_var, width=36)),
            ("Вес", ttk.Entry(frm, textvariable=weight_var, width=36)),
            ("Штрихкод", ttk.Entry(frm, textvariable=barcode_var, width=36)),
            ("Серийный учёт", ttk.Combobox(frm, textvariable=serial_var, values=["Да", "Нет"], state="readonly", width=33)),
            ("Категория", ttk.Combobox(frm, textvariable=category_var, values=list(categories.keys()), state="readonly", width=33)),
        ]

        row = 0
        for label, widget in fields:
            ttk.Label(frm, text=label).grid(row=row, column=0, sticky="w", pady=4)
            widget.grid(row=row, column=1, sticky="w", pady=4)
            row += 1

        ttk.Label(frm, text="Подкатегория").grid(row=row, column=0, sticky="w", pady=4)
        subcategory_box = ttk.Combobox(frm, textvariable=subcategory_var, values=list(subcategories.keys()), state="readonly", width=33)
        subcategory_box.grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        ttk.Label(frm, text="Продакт").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(frm, textvariable=product_owner_var, width=36, state="readonly").grid(row=row, column=1, sticky="w", pady=4)

        def read_id(token):
            if token and "|" in token:
                return int(token.split(" | ")[0])
            return None

        if mode == "edit" and product_id is not None:
            product = self.db.query(
                """
                SELECT brand, supplier_id, client_id, unit, volume, weight, barcode, serial_tracking,
                       category_id, subcategory_id, product_owner
                FROM products WHERE id = ?
                """,
                (product_id,),
            )[0]
            brand_var.set(product[0] or "")
            supplier_var.set(next((k for k, v in suppliers.items() if v == product[1]), ""))
            client_var.set(next((k for k, v in clients.items() if v == product[2]), ""))
            unit_var.set(product[3] or "Шт")
            volume_var.set("" if product[4] is None else str(product[4]))
            weight_var.set("" if product[5] is None else str(product[5]))
            barcode_var.set(product[6] or "")
            serial_var.set(product[7] or "Нет")
            category_var.set(next((k for k, v in categories.items() if v == product[8]), ""))
            load_all_subcategories()
            subcategory_var.set(next((k for k, v in subcategories.items() if v == product[9]), ""))
            product_owner_var.set(product[10] or self.current_user)

        load_all_subcategories()

        def on_save():
            brand = brand_var.get().strip()
            if not brand:
                messagebox.showwarning("Валидация", "Марка обязательна", parent=dialog)
                return

            supplier_id = read_id(supplier_var.get())
            client_id = read_id(client_var.get())
            if not supplier_id or not client_id:
                messagebox.showwarning("Валидация", "Укажите поставщика и 3PL клиента", parent=dialog)
                return

            category_id = read_id(category_var.get())
            subcategory_id = read_id(subcategory_var.get())
            if not category_id or not subcategory_id:
                messagebox.showwarning("Валидация", "Выберите категорию и подкатегорию", parent=dialog)
                return

            try:
                volume = float(volume_var.get()) if volume_var.get().strip() else None
                weight = float(weight_var.get()) if weight_var.get().strip() else None
            except ValueError:
                messagebox.showwarning("Валидация", "Объём и вес должны быть числами", parent=dialog)
                return

            payload = (
                brand,
                supplier_id,
                client_id,
                unit_var.get().strip(),
                volume,
                weight,
                barcode_var.get().strip(),
                serial_var.get().strip(),
                category_id,
                subcategory_id,
                product_owner_var.get().strip(),
            )

            try:
                if mode == "create":
                    self.db.execute(
                        """
                        INSERT INTO products(
                            name, brand, supplier_id, client_id, unit,
                            volume, weight, barcode, serial_tracking, category_id, subcategory_id,
                            product_owner, created_at
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (brand,) + payload + (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),),
                    )
                else:
                    self.db.execute(
                        """
                        UPDATE products
                        SET name=?, brand=?, supplier_id=?, client_id=?, unit=?,
                            volume=?, weight=?, barcode=?, serial_tracking=?, category_id=?, subcategory_id=?,
                            product_owner=?
                        WHERE id=?
                        """,
                        (brand,) + payload + (product_id,),
                    )
            except sqlite3.IntegrityError as exc:
                messagebox.showerror("Ошибка", f"Не удалось сохранить карточку: {exc}", parent=dialog)
                return

            self.refresh_all()
            dialog.destroy()

        ttk.Button(frm, text="Сохранить", command=on_save).grid(row=row + 1, column=1, sticky="e", pady=(14, 0))

    def delete_selected_product(self):
        selected = self.nomenclature_tree.selection()
        if not selected:
            messagebox.showwarning("Валидация", "Выберите карточку для удаления")
            return
        item = self.nomenclature_tree.item(selected[0], "values")
        product_id, brand = int(item[0]), item[1]
        if not messagebox.askyesno("Подтверждение", f"Удалить карточку товара {brand}?"):
            return
        self.db.execute("DELETE FROM products WHERE id=?", (product_id,))
        self.refresh_all()

    def show_copy_menu(self, event):
        row = self.nomenclature_tree.identify_row(event.y)
        col = self.nomenclature_tree.identify_column(event.x)
        if not row:
            return
        self.nomenclature_tree.selection_set(row)
        values = self.nomenclature_tree.item(row, "values")
        col_index = int(col.replace("#", "")) - 1 if col else 0
        self.selected_copy_value = values[col_index] if 0 <= col_index < len(values) else ""
        self.copy_menu.tk_popup(event.x_root, event.y_root)

    def copy_selected_value(self):
        self.clipboard_clear()
        self.clipboard_append(str(self.selected_copy_value))

    def _current_stock_by_product(self):
        rows = self.db.query(
            """
            SELECT p.id, COALESCE(SUM(CASE WHEN m.movement_type = 'IN' THEN m.quantity ELSE -m.quantity END), 0)
            FROM products p
            LEFT JOIN movements m ON m.product_id = p.id
            GROUP BY p.id
            """
        )
        return {pid: stock for pid, stock in rows}

    def add_movement(self):
        token = self.movement_product.get().strip()
        if not token:
            messagebox.showwarning("Валидация", "Выберите товар")
            return
        product_id = int(token.split(" | ")[0])
        movement_type = self.movement_type.get().strip()
        try:
            quantity = int(self.movement_qty.get())
            if quantity <= 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Валидация", "Количество должно быть положительным числом")
            return

        if movement_type == "OUT":
            available = self._current_stock_by_product().get(product_id, 0)
            if quantity > available:
                messagebox.showerror("Недостаточно остатка", f"Невозможно списать {quantity}. Доступно только {available}.")
                return

        self.db.execute(
            "INSERT INTO movements(product_id, movement_type, quantity, reference, moved_at, note) VALUES(?, ?, ?, ?, ?, ?)",
            (
                product_id,
                movement_type,
                quantity,
                self.movement_ref.get().strip(),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                self.movement_note.get().strip(),
            ),
        )

        self.movement_qty.set("1")
        self.movement_ref.set("")
        self.movement_note.set("")
        self.refresh_all()

    def refresh_all(self):
        self.refresh_suppliers()
        self.refresh_3pl_clients()
        self.refresh_categories_tab()
        self.refresh_nomenclature()
        self.refresh_inbound_orders()
        self.refresh_movements()
        self.refresh_stock()
        self.refresh_metrics()

    def refresh_suppliers(self):
        self._clear_tree(self.suppliers_tree)
        query = "SELECT id, name, COALESCE(phone, ''), COALESCE(created_at, '') FROM suppliers ORDER BY id DESC"
        rows = self.db.query(query)
        term = self.suppliers_search_var.get().strip().lower()
        for row in rows:
            if term and term not in (row[1] or '').lower():
                continue
            self.suppliers_tree.insert("", "end", values=row)

    def refresh_3pl_clients(self):
        self._clear_tree(self.clients_tree)
        rows = self.db.query("SELECT id, name, COALESCE(contact, ''), COALESCE(created_at, '') FROM clients ORDER BY id DESC")
        term = self.clients_search_var.get().strip().lower()
        for row in rows:
            if term and term not in (row[1] or '').lower():
                continue
            self.clients_tree.insert("", "end", values=row)

    def refresh_categories_tab(self):
        if not hasattr(self, "categories_tree"):
            return
        self._clear_tree(self.categories_tree)
        self._clear_tree(self.subcategories_tree)

        term = self.categories_filter_var.get().strip().lower()
        cat_rows = self.db.query("SELECT id, name FROM categories ORDER BY name")
        sub_rows = self.db.query(
            """
            SELECT s.id, s.name, COALESCE(c.name, '')
            FROM subcategories s
            LEFT JOIN categories c ON c.id = s.category_id
            ORDER BY c.name, s.name
            """
        )

        for row in cat_rows:
            if term and term not in (row[1] or '').lower():
                continue
            self.categories_tree.insert("", "end", values=row)

        for row in sub_rows:
            full = f"{row[1]} {row[2]}".lower()
            if term and term not in full:
                continue
            self.subcategories_tree.insert("", "end", values=row)

        parent_values = [f"{r[0]} | {r[1]}" for r in cat_rows]
        self.subcategory_parent_box["values"] = parent_values
        if parent_values and not self.subcategory_parent_var.get():
            self.subcategory_parent_var.set(parent_values[0])

    def refresh_nomenclature(self):
        self._clear_tree(self.nomenclature_tree)
        search_brand = self.nomenclature_brand_filter.get().strip().lower()
        rows = self.db.query(
            """
            SELECT p.id, p.brand, s.name, c.name, p.unit, p.volume, p.weight, p.barcode,
                   p.serial_tracking, cat.name, sub.name, p.product_owner
            FROM products p
            LEFT JOIN suppliers s ON s.id = p.supplier_id
            LEFT JOIN clients c ON c.id = p.client_id
            LEFT JOIN categories cat ON cat.id = p.category_id
            LEFT JOIN subcategories sub ON sub.id = p.subcategory_id
            ORDER BY p.id DESC
            """
        )
        for row in rows:
            brand = (row[1] or "").lower()
            if search_brand and search_brand not in brand:
                continue
            self.nomenclature_tree.insert("", "end", values=row)

        product_values = [
            f"{r[0]} | {r[1] or ''}" for r in self.db.query("SELECT id, brand FROM products ORDER BY id DESC")
        ]
        self.movement_product_box["values"] = product_values
        if product_values and not self.movement_product.get():
            self.movement_product.set(product_values[0])

    def refresh_inbound_orders(self):
        self._clear_tree(self.inbound_tree)
        rows = self.db.query(
            """
            SELECT o.order_number,
                   o.created_at,
                   COALESCE(o.received_at, ''),
                   o.created_by,
                   s.name,
                   c.name,
                   w.name,
                   o.status,
                   COALESCE(SUM(i.planned_qty), 0),
                   COALESCE(SUM(i.actual_qty), 0)
            FROM inbound_orders o
            JOIN suppliers s ON s.id = o.supplier_id
            JOIN clients c ON c.id = o.client_id
            JOIN warehouses w ON w.id = o.warehouse_id
            LEFT JOIN inbound_order_items i ON i.order_id = o.id
            GROUP BY o.id, o.order_number, o.created_at, o.received_at, o.created_by,
                     s.name, c.name, w.name, o.status
            ORDER BY o.id DESC
            """
        )

        search = self.inbound_order_search_var.get().strip().lower()
        status = self.inbound_status_var.get().strip()
        date_filter = self.inbound_date_filter_var.get().strip()

        for row in rows:
            order_number = (row[0] or "").lower()
            created_at = row[1] or ""
            row_status = row[7] or ""
            if search and search not in order_number:
                continue
            if status and status != "Все" and row_status != status:
                continue
            if date_filter and not created_at.startswith(date_filter):
                continue
            self.inbound_tree.insert("", "end", values=row)

    def refresh_movements(self):
        self._clear_tree(self.movements_tree)
        rows = self.db.query(
            """
            SELECT m.id, p.brand, m.movement_type, m.quantity, m.reference, m.moved_at, m.note
            FROM movements m
            JOIN products p ON p.id = m.product_id
            ORDER BY m.id DESC
            """
        )
        for row in rows:
            self.movements_tree.insert("", "end", values=row)

    def refresh_stock(self):
        self._clear_tree(self.stock_tree)
        rows = self.db.query(
            """
            SELECT p.brand, c.name, p.unit,
                   COALESCE(SUM(CASE WHEN m.movement_type = 'IN' THEN m.quantity ELSE -m.quantity END), 0)
            FROM products p
            LEFT JOIN clients c ON c.id = p.client_id
            LEFT JOIN movements m ON m.product_id = p.id
            GROUP BY p.id, p.brand, c.name, p.unit
            ORDER BY p.id DESC
            """
        )
        for row in rows:
            self.stock_tree.insert("", "end", values=row)

    def refresh_metrics(self):
        suppliers = self.db.query("SELECT COUNT(*) FROM suppliers")[0][0]
        products = self.db.query("SELECT COUNT(*) FROM products")[0][0]
        movements = self.db.query("SELECT COUNT(*) FROM movements")[0][0]
        inbound_orders = self.db.query("SELECT COUNT(*) FROM inbound_orders")[0][0]
        clients = self.db.query("SELECT COUNT(*) FROM clients")[0][0]
        self.metrics_var.set(
            f"Поставщики: {suppliers}   •   3PL клиенты: {clients}   •   Номенклатура: {products}   •   Приходы: {inbound_orders}   •   Проводок: {movements}"
        )

    def on_close(self):
        self.db.close()
        self.destroy()


if __name__ == "__main__":
    app = WMSApp()
    app.mainloop()
