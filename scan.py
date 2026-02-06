import getpass
import sqlite3
from contextlib import closing
from datetime import datetime
import tkinter as tk
from tkinter import messagebox, ttk

DB_FILE = "wms_3pl.db"


class Database:
    def __init__(self, db_file: str):
        self.conn = sqlite3.connect(db_file)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._create_schema()
        self._migrate_products_table()
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
        # SQLite does not allow adding a UNIQUE column via ALTER TABLE.
        # Add plain TEXT first, then create a unique index.
        self._add_column_if_missing("products", "article", "TEXT")
        self._add_column_if_missing("products", "brand", "TEXT")
        self._add_column_if_missing("products", "supplier_id", "INTEGER REFERENCES suppliers(id) ON DELETE SET NULL")
        self._add_column_if_missing("products", "volume", "REAL")
        self._add_column_if_missing("products", "weight", "REAL")
        self._add_column_if_missing("products", "barcode", "TEXT")
        self._add_column_if_missing("products", "serial_tracking", "TEXT NOT NULL DEFAULT 'Нет'")
        self._add_column_if_missing("products", "category_id", "INTEGER REFERENCES categories(id) ON DELETE SET NULL")
        self._add_column_if_missing("products", "subcategory_id", "INTEGER REFERENCES subcategories(id) ON DELETE SET NULL")
        self._add_column_if_missing("products", "product_owner", "TEXT")

        # backfill article from sku for old records
        self.execute("UPDATE products SET article = sku WHERE article IS NULL AND sku IS NOT NULL")
        self._add_unique_index_if_missing("products", "article")

    def _seed_reference_data(self):
        if not self.query("SELECT id FROM suppliers LIMIT 1"):
            self.execute("INSERT INTO suppliers(name) VALUES(?)", ("Default Supplier",))

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
        ttk.Label(header, text="Складская операционная система: клиенты, номенклатура, движения и остатки", style="Subtitle.TLabel").pack(anchor="w", pady=(2, 14))

        self.metrics_var = tk.StringVar(value="")
        metrics_card = ttk.Frame(root, style="Panel.TFrame", padding=14)
        metrics_card.pack(fill="x", pady=(0, 12))
        ttk.Label(metrics_card, textvariable=self.metrics_var, style="Section.TLabel").pack(anchor="w")

        notebook = ttk.Notebook(root)
        notebook.pack(fill="both", expand=True)

        self.clients_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)
        self.nomenclature_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)
        self.movements_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)
        self.stock_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)

        notebook.add(self.clients_tab, text="Клиенты")
        notebook.add(self.nomenclature_tab, text="Номенклатура")
        notebook.add(self.movements_tab, text="Движения")
        notebook.add(self.stock_tab, text="Остатки")

        self._build_clients_tab()
        self._build_nomenclature_tab()
        self._build_movements_tab()
        self._build_stock_tab()

    def _build_clients_tab(self):
        form = ttk.Frame(self.clients_tab, style="Panel.TFrame")
        form.pack(fill="x", pady=(0, 12))

        ttk.Label(form, text="Код клиента").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Label(form, text="Название").grid(row=0, column=2, sticky="w", padx=(0, 8), pady=4)
        ttk.Label(form, text="Контакт").grid(row=0, column=4, sticky="w", padx=(0, 8), pady=4)

        self.client_code = tk.StringVar()
        self.client_name = tk.StringVar()
        self.client_contact = tk.StringVar()

        ttk.Entry(form, textvariable=self.client_code, width=18).grid(row=0, column=1, padx=(0, 16), pady=4)
        ttk.Entry(form, textvariable=self.client_name, width=34).grid(row=0, column=3, padx=(0, 16), pady=4)
        ttk.Entry(form, textvariable=self.client_contact, width=30).grid(row=0, column=5, padx=(0, 16), pady=4)
        ttk.Button(form, text="Добавить клиента", command=self.add_client).grid(row=0, column=6)

        self.clients_tree = ttk.Treeview(self.clients_tab, columns=("id", "code", "name", "contact", "created"), show="headings")
        self.clients_tree.pack(fill="both", expand=True)
        for col, title, width in [("id", "ID", 60), ("code", "Код", 140), ("name", "Клиент", 300), ("contact", "Контакт", 240), ("created", "Создан", 180)]:
            self.clients_tree.heading(col, text=title)
            self.clients_tree.column(col, width=width, anchor="w")

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
            "article",
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
            ("article", "Артикул", 110),
            ("brand", "Марка", 140),
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

    def _build_movements_tab(self):
        form = ttk.Frame(self.movements_tab, style="Panel.TFrame")
        form.pack(fill="x", pady=(0, 12))

        ttk.Label(form, text="Артикул").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=4)
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

        self.movements_tree = ttk.Treeview(self.movements_tab, columns=("id", "article", "brand", "type", "qty", "reference", "date", "note"), show="headings")
        self.movements_tree.pack(fill="both", expand=True)
        for col, title, width in [("id", "ID", 60), ("article", "Артикул", 110), ("brand", "Марка", 170), ("type", "Тип", 80), ("qty", "Кол-во", 90), ("reference", "Документ", 140), ("date", "Дата", 180), ("note", "Комментарий", 230)]:
            self.movements_tree.heading(col, text=title)
            self.movements_tree.column(col, width=width, anchor="w")

    def _build_stock_tab(self):
        top = ttk.Frame(self.stock_tab, style="Panel.TFrame")
        top.pack(fill="x", pady=(0, 10))
        ttk.Label(top, text="Онлайн остатки по номенклатуре", style="Section.TLabel").pack(side="left")
        ttk.Button(top, text="Обновить", command=self.refresh_stock).pack(side="right")

        self.stock_tree = ttk.Treeview(self.stock_tab, columns=("article", "brand", "client", "unit", "stock"), show="headings")
        self.stock_tree.pack(fill="both", expand=True)
        for col, title, width in [("article", "Артикул", 120), ("brand", "Марка", 240), ("client", "3PL клиент", 220), ("unit", "Ед.", 80), ("stock", "Остаток", 100)]:
            self.stock_tree.heading(col, text=title)
            self.stock_tree.column(col, width=width, anchor="w")

    def _clear_tree(self, tree):
        for row in tree.get_children():
            tree.delete(row)

    def add_client(self):
        code = self.client_code.get().strip().upper()
        name = self.client_name.get().strip()
        contact = self.client_contact.get().strip()
        if not code or not name:
            messagebox.showwarning("Валидация", "Введите код и название клиента")
            return
        try:
            self.db.execute("INSERT INTO clients(code, name, contact, created_at) VALUES(?, ?, ?, ?)", (code, name, contact, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            self.client_code.set("")
            self.client_name.set("")
            self.client_contact.set("")
            self.refresh_all()
        except sqlite3.IntegrityError:
            messagebox.showerror("Ошибка", "Клиент с таким кодом уже существует")

    def _next_article(self):
        row = self.db.query("SELECT article FROM products WHERE article LIKE 'ART%' ORDER BY id DESC LIMIT 1")
        if not row or not row[0][0]:
            return "ART00001"
        value = row[0][0]
        try:
            num = int(value.replace("ART", "")) + 1
        except ValueError:
            num = self.db.query("SELECT COUNT(*) FROM products")[0][0] + 1
        return f"ART{num:05d}"

    def _load_reference_dict(self, table_name):
        rows = self.db.query(f"SELECT id, name FROM {table_name} ORDER BY name")
        return {f"{r[0]} | {r[1]}": r[0] for r in rows}

    def _create_category_or_subcategory(self, table, name, category_id=None):
        name = name.strip()
        if not name:
            return None
        try:
            if table == "categories":
                return self.db.execute("INSERT INTO categories(name) VALUES(?)", (name,))
            return self.db.execute("INSERT INTO subcategories(category_id, name) VALUES(?, ?)", (category_id, name))
        except sqlite3.IntegrityError:
            if table == "categories":
                return self.db.query("SELECT id FROM categories WHERE name = ?", (name,))[0][0]
            return self.db.query("SELECT id FROM subcategories WHERE name = ? AND category_id IS ?", (name, category_id))[0][0]

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

        article_var = tk.StringVar(value=self._next_article())
        brand_var = tk.StringVar()
        supplier_var = tk.StringVar()
        client_var = tk.StringVar()
        unit_var = tk.StringVar(value="Шт")
        volume_var = tk.StringVar()
        weight_var = tk.StringVar()
        barcode_var = tk.StringVar()
        serial_var = tk.StringVar(value="Нет")
        category_var = tk.StringVar()
        new_category_var = tk.StringVar()
        subcategory_var = tk.StringVar()
        new_subcategory_var = tk.StringVar()
        product_owner_var = tk.StringVar(value=self.current_user)

        suppliers = self._load_reference_dict("suppliers")
        clients = self._load_reference_dict("clients")
        categories = self._load_reference_dict("categories")
        subcategories = self._load_reference_dict("subcategories")

        def refill_subcategories(*_):
            selected = category_var.get().strip()
            cat_id = int(selected.split(" | ")[0]) if selected and "|" in selected else None
            rows = self.db.query(
                "SELECT id, name FROM subcategories WHERE (? IS NULL OR category_id = ?) ORDER BY name",
                (cat_id, cat_id),
            )
            options = [f"{r[0]} | {r[1]}" for r in rows]
            subcategory_box["values"] = options
            if options and not subcategory_var.get():
                subcategory_var.set(options[0])

        fields = [
            ("Марка", ttk.Entry(frm, textvariable=brand_var, width=36)),
            ("Артикул", ttk.Entry(frm, textvariable=article_var, width=36, state="readonly")),
            ("Поставщик", ttk.Combobox(frm, textvariable=supplier_var, values=list(suppliers.keys()), state="readonly", width=33)),
            ("3PL клиент", ttk.Combobox(frm, textvariable=client_var, values=list(clients.keys()), state="readonly", width=33)),
            ("Единица измерения", ttk.Combobox(frm, textvariable=unit_var, values=["Шт", "Палета"], state="readonly", width=33)),
            ("Объём", ttk.Entry(frm, textvariable=volume_var, width=36)),
            ("Вес", ttk.Entry(frm, textvariable=weight_var, width=36)),
            ("Штрихкод", ttk.Entry(frm, textvariable=barcode_var, width=36)),
            ("Серийный учёт", ttk.Combobox(frm, textvariable=serial_var, values=["Да", "Нет"], state="readonly", width=33)),
            ("Категория (выбрать)", ttk.Combobox(frm, textvariable=category_var, values=list(categories.keys()), state="readonly", width=33)),
            ("Категория (новая)", ttk.Entry(frm, textvariable=new_category_var, width=36)),
        ]

        row = 0
        for label, widget in fields:
            ttk.Label(frm, text=label).grid(row=row, column=0, sticky="w", pady=4)
            widget.grid(row=row, column=1, sticky="w", pady=4)
            row += 1

        ttk.Label(frm, text="Подкатегория (выбрать)").grid(row=row, column=0, sticky="w", pady=4)
        subcategory_box = ttk.Combobox(frm, textvariable=subcategory_var, values=list(subcategories.keys()), state="readonly", width=33)
        subcategory_box.grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        ttk.Label(frm, text="Подкатегория (новая)").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(frm, textvariable=new_subcategory_var, width=36).grid(row=row, column=1, sticky="w", pady=4)
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
                SELECT article, brand, supplier_id, client_id, unit, volume, weight, barcode, serial_tracking,
                       category_id, subcategory_id, product_owner
                FROM products WHERE id = ?
                """,
                (product_id,),
            )[0]
            article_var.set(product[0] or "")
            brand_var.set(product[1] or "")
            supplier_var.set(next((k for k, v in suppliers.items() if v == product[2]), ""))
            client_var.set(next((k for k, v in clients.items() if v == product[3]), ""))
            unit_var.set(product[4] or "Шт")
            volume_var.set("" if product[5] is None else str(product[5]))
            weight_var.set("" if product[6] is None else str(product[6]))
            barcode_var.set(product[7] or "")
            serial_var.set(product[8] or "Нет")
            category_var.set(next((k for k, v in categories.items() if v == product[9]), ""))
            refill_subcategories()
            subcategory_var.set(next((k for k, v in subcategories.items() if v == product[10]), ""))
            product_owner_var.set(product[11] or self.current_user)

        category_var.trace_add("write", refill_subcategories)

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
            if new_category_var.get().strip():
                category_id = self._create_category_or_subcategory("categories", new_category_var.get().strip())

            subcategory_id = read_id(subcategory_var.get())
            if new_subcategory_var.get().strip():
                subcategory_id = self._create_category_or_subcategory(
                    "subcategories",
                    new_subcategory_var.get().strip(),
                    category_id,
                )

            try:
                volume = float(volume_var.get()) if volume_var.get().strip() else None
                weight = float(weight_var.get()) if weight_var.get().strip() else None
            except ValueError:
                messagebox.showwarning("Валидация", "Объём и вес должны быть числами", parent=dialog)
                return

            payload = (
                article_var.get().strip(),
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
                            article, sku, name, brand, supplier_id, client_id, unit,
                            volume, weight, barcode, serial_tracking, category_id, subcategory_id,
                            product_owner, created_at
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        payload + (article_var.get().strip(), brand, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                    )
                else:
                    self.db.execute(
                        """
                        UPDATE products
                        SET article=?, sku=?, name=?, brand=?, supplier_id=?, client_id=?, unit=?,
                            volume=?, weight=?, barcode=?, serial_tracking=?, category_id=?, subcategory_id=?,
                            product_owner=?
                        WHERE id=?
                        """,
                        payload + (article_var.get().strip(), brand, product_id),
                    )
            except sqlite3.IntegrityError:
                messagebox.showerror("Ошибка", "Артикул должен быть уникальным", parent=dialog)
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
        product_id, article = int(item[0]), item[1]
        if not messagebox.askyesno("Подтверждение", f"Удалить карточку товара {article}?"):
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
            messagebox.showwarning("Валидация", "Выберите артикул")
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
        self.refresh_clients()
        self.refresh_nomenclature()
        self.refresh_movements()
        self.refresh_stock()
        self.refresh_metrics()

    def refresh_clients(self):
        self._clear_tree(self.clients_tree)
        rows = self.db.query("SELECT id, code, name, contact, created_at FROM clients ORDER BY id DESC")
        for row in rows:
            self.clients_tree.insert("", "end", values=row)

    def refresh_nomenclature(self):
        self._clear_tree(self.nomenclature_tree)
        search_brand = self.nomenclature_brand_filter.get().strip().lower()
        rows = self.db.query(
            """
            SELECT p.id, p.article, p.brand, s.name, c.name, p.unit, p.volume, p.weight, p.barcode,
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
            brand = (row[2] or "").lower()
            if search_brand and search_brand not in brand:
                continue
            self.nomenclature_tree.insert("", "end", values=row)

        product_values = [
            f"{r[0]} | {r[1] or ''} | {r[2] or ''}" for r in self.db.query("SELECT id, article, brand FROM products ORDER BY id DESC")
        ]
        self.movement_product_box["values"] = product_values
        if product_values and not self.movement_product.get():
            self.movement_product.set(product_values[0])

    def refresh_movements(self):
        self._clear_tree(self.movements_tree)
        rows = self.db.query(
            """
            SELECT m.id, p.article, p.brand, m.movement_type, m.quantity, m.reference, m.moved_at, m.note
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
            SELECT p.article, p.brand, c.name, p.unit,
                   COALESCE(SUM(CASE WHEN m.movement_type = 'IN' THEN m.quantity ELSE -m.quantity END), 0)
            FROM products p
            LEFT JOIN clients c ON c.id = p.client_id
            LEFT JOIN movements m ON m.product_id = p.id
            GROUP BY p.id, p.article, p.brand, c.name, p.unit
            ORDER BY p.id DESC
            """
        )
        for row in rows:
            self.stock_tree.insert("", "end", values=row)

    def refresh_metrics(self):
        clients = self.db.query("SELECT COUNT(*) FROM clients")[0][0]
        products = self.db.query("SELECT COUNT(*) FROM products")[0][0]
        movements = self.db.query("SELECT COUNT(*) FROM movements")[0][0]
        self.metrics_var.set(f"Клиенты: {clients}   •   Номенклатура: {products}   •   Проводок: {movements}")

    def on_close(self):
        self.db.close()
        self.destroy()


if __name__ == "__main__":
    app = WMSApp()
    app.mainloop()
