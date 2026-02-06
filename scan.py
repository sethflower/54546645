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

                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sku TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    client_id INTEGER NOT NULL,
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
        self.geometry("1250x760")
        self.minsize(1100, 680)

        self.db = Database(DB_FILE)
        self.style = ttk.Style(self)
        self._configure_style()

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
            "danger": "#B02A37",
        }

        self.colors = palette

        self.style.configure("TFrame", background=palette["bg"])
        self.style.configure("Panel.TFrame", background=palette["panel"])
        self.style.configure(
            "Header.TLabel",
            background=palette["bg"],
            foreground=palette["text"],
            font=("Segoe UI", 20, "bold"),
        )
        self.style.configure(
            "Subtitle.TLabel",
            background=palette["bg"],
            foreground=palette["muted"],
            font=("Segoe UI", 10),
        )
        self.style.configure(
            "Section.TLabel",
            background=palette["panel"],
            foreground=palette["text"],
            font=("Segoe UI", 12, "bold"),
        )
        self.style.configure(
            "TLabel",
            background=palette["panel"],
            foreground=palette["text"],
            font=("Segoe UI", 10),
        )
        self.style.configure(
            "TButton",
            font=("Segoe UI", 10, "bold"),
            padding=8,
            foreground="#FFFFFF",
            background=palette["accent"],
            borderwidth=0,
        )
        self.style.map("TButton", background=[("active", palette["accent2"])])

        self.style.configure("Treeview", rowheight=28, font=("Segoe UI", 10))
        self.style.configure(
            "Treeview.Heading",
            font=("Segoe UI", 10, "bold"),
            background=palette["accent"],
            foreground="#FFFFFF",
            padding=6,
        )
        self.style.map("Treeview", background=[("selected", "#D7E8FB")], foreground=[("selected", "#122033")])
        self.style.configure("TNotebook", background=palette["bg"], borderwidth=0)
        self.style.configure("TNotebook.Tab", padding=(16, 10), font=("Segoe UI", 10, "bold"))

    def _build_layout(self):
        root = ttk.Frame(self, padding=18)
        root.pack(fill="both", expand=True)

        header = ttk.Frame(root)
        header.pack(fill="x")

        ttk.Label(header, text="Warehouse Management System (3PL)", style="Header.TLabel").pack(anchor="w")
        ttk.Label(
            header,
            text="Складская операционная система: клиенты, SKU, входящие/исходящие движения и контроль остатков",
            style="Subtitle.TLabel",
        ).pack(anchor="w", pady=(2, 14))

        self.metrics_var = tk.StringVar(value="")
        metrics_card = ttk.Frame(root, style="Panel.TFrame", padding=14)
        metrics_card.pack(fill="x", pady=(0, 12))
        ttk.Label(metrics_card, textvariable=self.metrics_var, style="Section.TLabel").pack(anchor="w")

        notebook = ttk.Notebook(root)
        notebook.pack(fill="both", expand=True)

        self.clients_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)
        self.products_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)
        self.movements_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)
        self.stock_tab = ttk.Frame(notebook, style="Panel.TFrame", padding=14)

        notebook.add(self.clients_tab, text="Клиенты")
        notebook.add(self.products_tab, text="Товары / SKU")
        notebook.add(self.movements_tab, text="Движения")
        notebook.add(self.stock_tab, text="Остатки")

        self._build_clients_tab()
        self._build_products_tab()
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

        self.clients_tree = ttk.Treeview(
            self.clients_tab,
            columns=("id", "code", "name", "contact", "created"),
            show="headings",
        )
        self.clients_tree.pack(fill="both", expand=True)

        for col, title, width in [
            ("id", "ID", 60),
            ("code", "Код", 140),
            ("name", "Клиент", 300),
            ("contact", "Контакт", 240),
            ("created", "Создан", 180),
        ]:
            self.clients_tree.heading(col, text=title)
            self.clients_tree.column(col, width=width, anchor="w")

    def _build_products_tab(self):
        form = ttk.Frame(self.products_tab, style="Panel.TFrame")
        form.pack(fill="x", pady=(0, 12))

        ttk.Label(form, text="SKU").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=4)
        ttk.Label(form, text="Наименование").grid(row=0, column=2, sticky="w", padx=(0, 6), pady=4)
        ttk.Label(form, text="Клиент").grid(row=0, column=4, sticky="w", padx=(0, 6), pady=4)
        ttk.Label(form, text="Ед.").grid(row=0, column=6, sticky="w", padx=(0, 6), pady=4)
        ttk.Label(form, text="Мин. остаток").grid(row=0, column=8, sticky="w", padx=(0, 6), pady=4)

        self.product_sku = tk.StringVar()
        self.product_name = tk.StringVar()
        self.product_client = tk.StringVar()
        self.product_unit = tk.StringVar(value="pcs")
        self.product_min_stock = tk.StringVar(value="0")

        ttk.Entry(form, textvariable=self.product_sku, width=14).grid(row=0, column=1, padx=(0, 12), pady=4)
        ttk.Entry(form, textvariable=self.product_name, width=30).grid(row=0, column=3, padx=(0, 12), pady=4)
        self.product_client_box = ttk.Combobox(form, textvariable=self.product_client, width=22, state="readonly")
        self.product_client_box.grid(row=0, column=5, padx=(0, 12), pady=4)
        ttk.Entry(form, textvariable=self.product_unit, width=8).grid(row=0, column=7, padx=(0, 12), pady=4)
        ttk.Entry(form, textvariable=self.product_min_stock, width=8).grid(row=0, column=9, padx=(0, 12), pady=4)
        ttk.Button(form, text="Добавить SKU", command=self.add_product).grid(row=0, column=10)

        self.products_tree = ttk.Treeview(
            self.products_tab,
            columns=("id", "sku", "name", "client", "unit", "min_stock", "created"),
            show="headings",
        )
        self.products_tree.pack(fill="both", expand=True)

        for col, title, width in [
            ("id", "ID", 60),
            ("sku", "SKU", 150),
            ("name", "Товар", 260),
            ("client", "Клиент", 220),
            ("unit", "Ед.", 80),
            ("min_stock", "Мин.", 80),
            ("created", "Создан", 180),
        ]:
            self.products_tree.heading(col, text=title)
            self.products_tree.column(col, width=width, anchor="w")

    def _build_movements_tab(self):
        form = ttk.Frame(self.movements_tab, style="Panel.TFrame")
        form.pack(fill="x", pady=(0, 12))

        ttk.Label(form, text="SKU").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=4)
        ttk.Label(form, text="Тип").grid(row=0, column=2, sticky="w", padx=(0, 6), pady=4)
        ttk.Label(form, text="Кол-во").grid(row=0, column=4, sticky="w", padx=(0, 6), pady=4)
        ttk.Label(form, text="Документ").grid(row=0, column=6, sticky="w", padx=(0, 6), pady=4)
        ttk.Label(form, text="Комментарий").grid(row=0, column=8, sticky="w", padx=(0, 6), pady=4)

        self.movement_product = tk.StringVar()
        self.movement_type = tk.StringVar(value="IN")
        self.movement_qty = tk.StringVar(value="1")
        self.movement_ref = tk.StringVar()
        self.movement_note = tk.StringVar()

        self.movement_product_box = ttk.Combobox(form, textvariable=self.movement_product, width=24, state="readonly")
        self.movement_product_box.grid(row=0, column=1, padx=(0, 12), pady=4)
        ttk.Combobox(form, textvariable=self.movement_type, values=["IN", "OUT"], width=8, state="readonly").grid(
            row=0, column=3, padx=(0, 12), pady=4
        )
        ttk.Entry(form, textvariable=self.movement_qty, width=10).grid(row=0, column=5, padx=(0, 12), pady=4)
        ttk.Entry(form, textvariable=self.movement_ref, width=16).grid(row=0, column=7, padx=(0, 12), pady=4)
        ttk.Entry(form, textvariable=self.movement_note, width=26).grid(row=0, column=9, padx=(0, 12), pady=4)
        ttk.Button(form, text="Провести", command=self.add_movement).grid(row=0, column=10)

        self.movements_tree = ttk.Treeview(
            self.movements_tab,
            columns=("id", "sku", "name", "type", "qty", "reference", "date", "note"),
            show="headings",
        )
        self.movements_tree.pack(fill="both", expand=True)

        for col, title, width in [
            ("id", "ID", 60),
            ("sku", "SKU", 130),
            ("name", "Товар", 230),
            ("type", "Тип", 80),
            ("qty", "Кол-во", 90),
            ("reference", "Документ", 140),
            ("date", "Дата", 180),
            ("note", "Комментарий", 230),
        ]:
            self.movements_tree.heading(col, text=title)
            self.movements_tree.column(col, width=width, anchor="w")

    def _build_stock_tab(self):
        top = ttk.Frame(self.stock_tab, style="Panel.TFrame")
        top.pack(fill="x", pady=(0, 10))
        ttk.Label(top, text="Онлайн остатки по SKU", style="Section.TLabel").pack(side="left")
        ttk.Button(top, text="Обновить", command=self.refresh_stock).pack(side="right")

        self.stock_tree = ttk.Treeview(
            self.stock_tab,
            columns=("sku", "name", "client", "unit", "min_stock", "stock", "status"),
            show="headings",
        )
        self.stock_tree.pack(fill="both", expand=True)

        for col, title, width in [
            ("sku", "SKU", 150),
            ("name", "Товар", 280),
            ("client", "Клиент", 230),
            ("unit", "Ед.", 80),
            ("min_stock", "Мин.", 90),
            ("stock", "Остаток", 100),
            ("status", "Статус", 140),
        ]:
            self.stock_tree.heading(col, text=title)
            self.stock_tree.column(col, width=width, anchor="w")

    def add_client(self):
        code = self.client_code.get().strip().upper()
        name = self.client_name.get().strip()
        contact = self.client_contact.get().strip()

        if not code or not name:
            messagebox.showwarning("Валидация", "Введите код и название клиента")
            return

        try:
            self.db.execute(
                "INSERT INTO clients(code, name, contact, created_at) VALUES(?, ?, ?, ?)",
                (code, name, contact, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )
            self.client_code.set("")
            self.client_name.set("")
            self.client_contact.set("")
            self.refresh_all()
        except sqlite3.IntegrityError:
            messagebox.showerror("Ошибка", "Клиент с таким кодом уже существует")

    def add_product(self):
        sku = self.product_sku.get().strip().upper()
        name = self.product_name.get().strip()
        client_token = self.product_client.get().strip()
        unit = self.product_unit.get().strip() or "pcs"

        if not sku or not name or not client_token:
            messagebox.showwarning("Валидация", "Заполните SKU, наименование и клиента")
            return

        try:
            min_stock = int(self.product_min_stock.get())
        except ValueError:
            messagebox.showwarning("Валидация", "Мин. остаток должен быть числом")
            return

        client_id = int(client_token.split(" | ")[0])

        try:
            self.db.execute(
                """
                INSERT INTO products(sku, name, client_id, unit, min_stock, created_at)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (sku, name, client_id, unit, min_stock, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )
            self.product_sku.set("")
            self.product_name.set("")
            self.product_unit.set("pcs")
            self.product_min_stock.set("0")
            self.refresh_all()
        except sqlite3.IntegrityError:
            messagebox.showerror("Ошибка", "SKU уже существует")

    def _current_stock_by_product(self):
        rows = self.db.query(
            """
            SELECT p.id,
                   COALESCE(SUM(CASE WHEN m.movement_type = 'IN' THEN m.quantity ELSE -m.quantity END), 0) AS stock
            FROM products p
            LEFT JOIN movements m ON m.product_id = p.id
            GROUP BY p.id
            """
        )
        return {pid: stock for pid, stock in rows}

    def add_movement(self):
        token = self.movement_product.get().strip()
        if not token:
            messagebox.showwarning("Валидация", "Выберите SKU")
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
            stock_map = self._current_stock_by_product()
            available = stock_map.get(product_id, 0)
            if quantity > available:
                messagebox.showerror(
                    "Недостаточно остатка",
                    f"Невозможно списать {quantity}. Доступно только {available}.",
                )
                return

        self.db.execute(
            """
            INSERT INTO movements(product_id, movement_type, quantity, reference, moved_at, note)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
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
        self.refresh_products()
        self.refresh_movements()
        self.refresh_stock()
        self.refresh_metrics()

    def _clear_tree(self, tree):
        for row in tree.get_children():
            tree.delete(row)

    def refresh_clients(self):
        self._clear_tree(self.clients_tree)
        rows = self.db.query("SELECT id, code, name, contact, created_at FROM clients ORDER BY id DESC")
        for row in rows:
            self.clients_tree.insert("", "end", values=row)

        client_values = [f"{r[0]} | {r[1]} | {r[2]}" for r in self.db.query("SELECT id, code, name FROM clients ORDER BY name")]
        self.product_client_box["values"] = client_values
        if client_values and not self.product_client.get():
            self.product_client.set(client_values[0])

    def refresh_products(self):
        self._clear_tree(self.products_tree)
        rows = self.db.query(
            """
            SELECT p.id, p.sku, p.name, c.name, p.unit, p.min_stock, p.created_at
            FROM products p
            JOIN clients c ON c.id = p.client_id
            ORDER BY p.id DESC
            """
        )
        for row in rows:
            self.products_tree.insert("", "end", values=row)

        product_values = [
            f"{r[0]} | {r[1]} | {r[2]}" for r in self.db.query("SELECT id, sku, name FROM products ORDER BY sku")
        ]
        self.movement_product_box["values"] = product_values
        if product_values and not self.movement_product.get():
            self.movement_product.set(product_values[0])

    def refresh_movements(self):
        self._clear_tree(self.movements_tree)
        rows = self.db.query(
            """
            SELECT m.id, p.sku, p.name, m.movement_type, m.quantity, m.reference, m.moved_at, m.note
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
            SELECT p.sku,
                   p.name,
                   c.name,
                   p.unit,
                   p.min_stock,
                   COALESCE(SUM(CASE WHEN m.movement_type = 'IN' THEN m.quantity ELSE -m.quantity END), 0) AS stock
            FROM products p
            JOIN clients c ON c.id = p.client_id
            LEFT JOIN movements m ON m.product_id = p.id
            GROUP BY p.id, p.sku, p.name, c.name, p.unit, p.min_stock
            ORDER BY c.name, p.sku
            """
        )
        for sku, name, client, unit, min_stock, stock in rows:
            status = "OK" if stock >= min_stock else "LOW"
            self.stock_tree.insert("", "end", values=(sku, name, client, unit, min_stock, stock, status))

    def refresh_metrics(self):
        clients = self.db.query("SELECT COUNT(*) FROM clients")[0][0]
        products = self.db.query("SELECT COUNT(*) FROM products")[0][0]
        movements = self.db.query("SELECT COUNT(*) FROM movements")[0][0]
        total_stock = self.db.query(
            """
            SELECT COALESCE(SUM(CASE WHEN movement_type = 'IN' THEN quantity ELSE -quantity END), 0)
            FROM movements
            """
        )[0][0]
        self.metrics_var.set(
            f"Клиенты: {clients}   •   SKU: {products}   •   Проводок: {movements}   •   Общий остаток: {total_stock}"
        )

    def on_close(self):
        self.db.close()
        self.destroy()


if __name__ == "__main__":
    app = WMSApp()
    app.mainloop()
