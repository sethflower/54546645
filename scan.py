import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import sqlite3
import uuid
import hashlib
import os
import csv
import datetime
import logging
import traceback
import tempfile
import webbrowser

APP_TITLE = "WMS — Складська система"
DB_FILE = "wms.sqlite"
LOG_FILE = "wms.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def generate_uuid() -> str:
    return str(uuid.uuid4())


def now_str() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def hash_password(password: str, salt: bytes | None = None) -> tuple[str, str]:
    if salt is None:
        salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 100_000)
    return salt.hex(), digest.hex()


def verify_password(password: str, salt_hex: str, digest_hex: str) -> bool:
    salt = bytes.fromhex(salt_hex)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 100_000)
    return digest.hex() == digest_hex


class DBManager:
    def __init__(self, db_file: str = DB_FILE):
        self.db_file = db_file
        self.connection = sqlite3.connect(self.db_file)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")

    def execute(self, sql: str, params: tuple | list = ()) -> sqlite3.Cursor:
        cur = self.connection.cursor()
        cur.execute(sql, params)
        return cur

    def executemany(self, sql: str, params: list[tuple]) -> sqlite3.Cursor:
        cur = self.connection.cursor()
        cur.executemany(sql, params)
        return cur

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def close(self) -> None:
        self.connection.close()

    def init_schema(self) -> None:
        schema_sql = """
        CREATE TABLE IF NOT EXISTS roles (
            id TEXT PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS role_permissions (
            id TEXT PRIMARY KEY,
            role_id TEXT NOT NULL,
            permission TEXT NOT NULL,
            FOREIGN KEY(role_id) REFERENCES roles(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            full_name TEXT NOT NULL,
            role_id TEXT NOT NULL,
            salt TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            failed_attempts INTEGER NOT NULL DEFAULT 0,
            locked_until TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(role_id) REFERENCES roles(id)
        );
        CREATE TABLE IF NOT EXISTS clients (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            code TEXT UNIQUE NOT NULL,
            contract TEXT,
            sla TEXT,
            billing_type TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS suppliers (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            code TEXT UNIQUE NOT NULL,
            contact TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS warehouses (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            code TEXT UNIQUE NOT NULL,
            address TEXT,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS zones (
            id TEXT PRIMARY KEY,
            warehouse_id TEXT NOT NULL,
            name TEXT NOT NULL,
            code TEXT NOT NULL,
            zone_type TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(warehouse_id) REFERENCES warehouses(id)
        );
        CREATE TABLE IF NOT EXISTS locations (
            id TEXT PRIMARY KEY,
            zone_id TEXT NOT NULL,
            code TEXT NOT NULL,
            location_type TEXT NOT NULL,
            capacity_volume REAL NOT NULL DEFAULT 0,
            capacity_weight REAL NOT NULL DEFAULT 0,
            capacity_pallets INTEGER NOT NULL DEFAULT 0,
            allowed_category TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(zone_id) REFERENCES zones(id)
        );
        CREATE TABLE IF NOT EXISTS items (
            id TEXT PRIMARY KEY,
            client_id TEXT NOT NULL,
            sku TEXT NOT NULL,
            name_ua TEXT NOT NULL,
            name_en TEXT,
            category TEXT,
            uom_base TEXT NOT NULL,
            uom_alt TEXT,
            weight REAL NOT NULL DEFAULT 0,
            length REAL NOT NULL DEFAULT 0,
            width REAL NOT NULL DEFAULT 0,
            height REAL NOT NULL DEFAULT 0,
            is_serial INTEGER NOT NULL DEFAULT 0,
            is_batch INTEGER NOT NULL DEFAULT 0,
            has_expiry INTEGER NOT NULL DEFAULT 0,
            temp_mode TEXT,
            storage_rules TEXT,
            status TEXT NOT NULL DEFAULT 'Активний',
            created_at TEXT NOT NULL,
            UNIQUE(client_id, sku),
            FOREIGN KEY(client_id) REFERENCES clients(id)
        );
        CREATE TABLE IF NOT EXISTS item_barcodes (
            id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            barcode TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(item_id, barcode),
            FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS item_photos (
            id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            photo BLOB,
            created_at TEXT NOT NULL,
            FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS inventory_balances (
            id TEXT PRIMARY KEY,
            client_id TEXT NOT NULL,
            warehouse_id TEXT NOT NULL,
            location_id TEXT,
            item_id TEXT NOT NULL,
            batch TEXT,
            serial TEXT,
            expiry_date TEXT,
            qty REAL NOT NULL,
            reserved_qty REAL NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            UNIQUE(client_id, warehouse_id, location_id, item_id, batch, serial, expiry_date),
            FOREIGN KEY(client_id) REFERENCES clients(id),
            FOREIGN KEY(warehouse_id) REFERENCES warehouses(id),
            FOREIGN KEY(location_id) REFERENCES locations(id),
            FOREIGN KEY(item_id) REFERENCES items(id)
        );
        CREATE TABLE IF NOT EXISTS stock_moves (
            id TEXT PRIMARY KEY,
            move_type TEXT NOT NULL,
            doc_type TEXT NOT NULL,
            doc_id TEXT NOT NULL,
            client_id TEXT NOT NULL,
            warehouse_id TEXT NOT NULL,
            location_from TEXT,
            location_to TEXT,
            item_id TEXT NOT NULL,
            batch TEXT,
            serial TEXT,
            expiry_date TEXT,
            qty REAL NOT NULL,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            note TEXT,
            FOREIGN KEY(client_id) REFERENCES clients(id),
            FOREIGN KEY(warehouse_id) REFERENCES warehouses(id),
            FOREIGN KEY(item_id) REFERENCES items(id)
        );
        CREATE TABLE IF NOT EXISTS inbound_orders (
            id TEXT PRIMARY KEY,
            number TEXT NOT NULL,
            status TEXT NOT NULL,
            client_id TEXT NOT NULL,
            supplier_id TEXT,
            warehouse_id TEXT NOT NULL,
            expected_date TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(number),
            FOREIGN KEY(client_id) REFERENCES clients(id),
            FOREIGN KEY(supplier_id) REFERENCES suppliers(id),
            FOREIGN KEY(warehouse_id) REFERENCES warehouses(id)
        );
        CREATE TABLE IF NOT EXISTS inbound_order_lines (
            id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            qty_plan REAL NOT NULL,
            qty_fact REAL NOT NULL DEFAULT 0,
            batch TEXT,
            serial TEXT,
            expiry_date TEXT,
            FOREIGN KEY(order_id) REFERENCES inbound_orders(id) ON DELETE CASCADE,
            FOREIGN KEY(item_id) REFERENCES items(id)
        );
        CREATE TABLE IF NOT EXISTS receipts (
            id TEXT PRIMARY KEY,
            number TEXT NOT NULL,
            status TEXT NOT NULL,
            order_id TEXT,
            client_id TEXT NOT NULL,
            warehouse_id TEXT NOT NULL,
            received_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES inbound_orders(id),
            FOREIGN KEY(client_id) REFERENCES clients(id),
            FOREIGN KEY(warehouse_id) REFERENCES warehouses(id)
        );
        CREATE TABLE IF NOT EXISTS receipt_lines (
            id TEXT PRIMARY KEY,
            receipt_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            qty REAL NOT NULL,
            batch TEXT,
            serial TEXT,
            expiry_date TEXT,
            FOREIGN KEY(receipt_id) REFERENCES receipts(id) ON DELETE CASCADE,
            FOREIGN KEY(item_id) REFERENCES items(id)
        );
        CREATE TABLE IF NOT EXISTS putaway_tasks (
            id TEXT PRIMARY KEY,
            receipt_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            qty REAL NOT NULL,
            location_to TEXT,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(receipt_id) REFERENCES receipts(id)
        );
        CREATE TABLE IF NOT EXISTS outbound_orders (
            id TEXT PRIMARY KEY,
            number TEXT NOT NULL,
            status TEXT NOT NULL,
            client_id TEXT NOT NULL,
            warehouse_id TEXT NOT NULL,
            ship_to TEXT,
            carrier TEXT,
            priority TEXT,
            deadline TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(number),
            FOREIGN KEY(client_id) REFERENCES clients(id),
            FOREIGN KEY(warehouse_id) REFERENCES warehouses(id)
        );
        CREATE TABLE IF NOT EXISTS outbound_order_lines (
            id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            qty_plan REAL NOT NULL,
            qty_reserved REAL NOT NULL DEFAULT 0,
            qty_picked REAL NOT NULL DEFAULT 0,
            FOREIGN KEY(order_id) REFERENCES outbound_orders(id) ON DELETE CASCADE,
            FOREIGN KEY(item_id) REFERENCES items(id)
        );
        CREATE TABLE IF NOT EXISTS pick_tasks (
            id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            qty REAL NOT NULL,
            location_from TEXT,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES outbound_orders(id)
        );
        CREATE TABLE IF NOT EXISTS pack_tasks (
            id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            qty REAL NOT NULL,
            package_type TEXT,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES outbound_orders(id)
        );
        CREATE TABLE IF NOT EXISTS shipments (
            id TEXT PRIMARY KEY,
            number TEXT NOT NULL,
            status TEXT NOT NULL,
            order_id TEXT NOT NULL,
            warehouse_id TEXT NOT NULL,
            shipped_at TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES outbound_orders(id)
        );
        CREATE TABLE IF NOT EXISTS shipment_lines (
            id TEXT PRIMARY KEY,
            shipment_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            qty REAL NOT NULL,
            FOREIGN KEY(shipment_id) REFERENCES shipments(id) ON DELETE CASCADE,
            FOREIGN KEY(item_id) REFERENCES items(id)
        );
        CREATE TABLE IF NOT EXISTS inventory_counts (
            id TEXT PRIMARY KEY,
            number TEXT NOT NULL,
            status TEXT NOT NULL,
            warehouse_id TEXT NOT NULL,
            zone_id TEXT,
            location_id TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(warehouse_id) REFERENCES warehouses(id)
        );
        CREATE TABLE IF NOT EXISTS inventory_count_lines (
            id TEXT PRIMARY KEY,
            count_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            qty_system REAL NOT NULL,
            qty_count REAL NOT NULL,
            FOREIGN KEY(count_id) REFERENCES inventory_counts(id) ON DELETE CASCADE,
            FOREIGN KEY(item_id) REFERENCES items(id)
        );
        CREATE TABLE IF NOT EXISTS returns (
            id TEXT PRIMARY KEY,
            number TEXT NOT NULL,
            status TEXT NOT NULL,
            client_id TEXT NOT NULL,
            warehouse_id TEXT NOT NULL,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(client_id) REFERENCES clients(id),
            FOREIGN KEY(warehouse_id) REFERENCES warehouses(id)
        );
        CREATE TABLE IF NOT EXISTS return_lines (
            id TEXT PRIMARY KEY,
            return_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            qty REAL NOT NULL,
            disposition TEXT NOT NULL,
            FOREIGN KEY(return_id) REFERENCES returns(id) ON DELETE CASCADE,
            FOREIGN KEY(item_id) REFERENCES items(id)
        );
        CREATE TABLE IF NOT EXISTS writeoffs (
            id TEXT PRIMARY KEY,
            number TEXT NOT NULL,
            status TEXT NOT NULL,
            warehouse_id TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(warehouse_id) REFERENCES warehouses(id)
        );
        CREATE TABLE IF NOT EXISTS writeoff_lines (
            id TEXT PRIMARY KEY,
            writeoff_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            qty REAL NOT NULL,
            FOREIGN KEY(writeoff_id) REFERENCES writeoffs(id) ON DELETE CASCADE,
            FOREIGN KEY(item_id) REFERENCES items(id)
        );
        CREATE TABLE IF NOT EXISTS audit_log (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            action TEXT NOT NULL,
            entity TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_items_client ON items(client_id);
        CREATE INDEX IF NOT EXISTS idx_moves_item ON stock_moves(item_id);
        CREATE INDEX IF NOT EXISTS idx_moves_doc ON stock_moves(doc_id);
        CREATE INDEX IF NOT EXISTS idx_inventory_item ON inventory_balances(item_id);
        """
        self.connection.executescript(schema_sql)
        self.connection.commit()


class DAL:
    def __init__(self, db: DBManager):
        self.db = db

    def get_roles(self) -> list[sqlite3.Row]:
        return self.db.execute("SELECT * FROM roles ORDER BY name").fetchall()

    def get_permissions_for_role(self, role_id: str) -> list[str]:
        rows = self.db.execute(
            "SELECT permission FROM role_permissions WHERE role_id = ?",
            (role_id,),
        ).fetchall()
        return [row["permission"] for row in rows]

    def get_user(self, username: str) -> sqlite3.Row | None:
        return self.db.execute(
            "SELECT * FROM users WHERE username = ?",
            (username,),
        ).fetchone()

    def list_items(self, client_id: str | None = None) -> list[sqlite3.Row]:
        if client_id:
            return self.db.execute(
                """
                SELECT items.*, clients.name AS client_name
                FROM items
                JOIN clients ON clients.id = items.client_id
                WHERE items.client_id = ?
                ORDER BY items.sku
                """,
                (client_id,),
            ).fetchall()
        return self.db.execute(
            """
            SELECT items.*, clients.name AS client_name
            FROM items
            JOIN clients ON clients.id = items.client_id
            ORDER BY items.sku
            """
        ).fetchall()

    def list_inbound_orders(self) -> list[sqlite3.Row]:
        return self.db.execute(
            """
            SELECT inbound_orders.*, clients.name AS client_name, suppliers.name AS supplier_name
            FROM inbound_orders
            JOIN clients ON clients.id = inbound_orders.client_id
            LEFT JOIN suppliers ON suppliers.id = inbound_orders.supplier_id
            ORDER BY inbound_orders.created_at DESC
            """
        ).fetchall()

    def list_outbound_orders(self) -> list[sqlite3.Row]:
        return self.db.execute(
            """
            SELECT outbound_orders.*, clients.name AS client_name
            FROM outbound_orders
            JOIN clients ON clients.id = outbound_orders.client_id
            ORDER BY outbound_orders.created_at DESC
            """
        ).fetchall()

    def list_inventory_balances(self) -> list[sqlite3.Row]:
        return self.db.execute(
            """
            SELECT inventory_balances.*, items.sku, items.name_ua, clients.name AS client_name,
                   warehouses.name AS warehouse_name
            FROM inventory_balances
            JOIN items ON items.id = inventory_balances.item_id
            JOIN clients ON clients.id = inventory_balances.client_id
            JOIN warehouses ON warehouses.id = inventory_balances.warehouse_id
            ORDER BY items.sku
            """
        ).fetchall()

    def insert_audit(self, user_id: str, action: str, entity: str, entity_id: str, description: str) -> None:
        self.db.execute(
            """
            INSERT INTO audit_log (id, user_id, action, entity, entity_id, description, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (generate_uuid(), user_id, action, entity, entity_id, description, now_str()),
        )
        self.db.commit()


class RBAC:
    def __init__(self, dal: DAL):
        self.dal = dal
        self.permissions: dict[str, set[str]] = {}

    def load(self) -> None:
        for role in self.dal.get_roles():
            perms = set(self.dal.get_permissions_for_role(role["id"]))
            self.permissions[role["id"]] = perms

    def has_permission(self, role_id: str, permission: str) -> bool:
        return permission in self.permissions.get(role_id, set())


class WMSService:
    def __init__(self, db: DBManager, dal: DAL):
        self.db = db
        self.dal = dal

    def create_stock_move(
        self,
        move_type: str,
        doc_type: str,
        doc_id: str,
        client_id: str,
        warehouse_id: str,
        item_id: str,
        qty: float,
        created_by: str,
        location_from: str | None = None,
        location_to: str | None = None,
        batch: str | None = None,
        serial: str | None = None,
        expiry_date: str | None = None,
        note: str | None = None,
    ) -> None:
        move_id = generate_uuid()
        self.db.execute(
            """
            INSERT INTO stock_moves (
                id, move_type, doc_type, doc_id, client_id, warehouse_id, location_from, location_to,
                item_id, batch, serial, expiry_date, qty, created_by, created_at, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                move_id,
                move_type,
                doc_type,
                doc_id,
                client_id,
                warehouse_id,
                location_from,
                location_to,
                item_id,
                batch,
                serial,
                expiry_date,
                qty,
                created_by,
                now_str(),
                note,
            ),
        )
        self.update_inventory_balance(
            client_id,
            warehouse_id,
            location_to,
            item_id,
            batch,
            serial,
            expiry_date,
            qty,
        )
        self.db.commit()

    def update_inventory_balance(
        self,
        client_id: str,
        warehouse_id: str,
        location_id: str | None,
        item_id: str,
        batch: str | None,
        serial: str | None,
        expiry_date: str | None,
        delta_qty: float,
    ) -> None:
        row = self.db.execute(
            """
            SELECT * FROM inventory_balances
            WHERE client_id = ? AND warehouse_id = ? AND location_id IS ? AND item_id = ?
                  AND batch IS ? AND serial IS ? AND expiry_date IS ?
            """,
            (client_id, warehouse_id, location_id, item_id, batch, serial, expiry_date),
        ).fetchone()
        if row:
            new_qty = row["qty"] + delta_qty
            if new_qty < 0:
                raise ValueError("Недостатньо залишків для операції")
            self.db.execute(
                """
                UPDATE inventory_balances
                SET qty = ?, updated_at = ?
                WHERE id = ?
                """,
                (new_qty, now_str(), row["id"]),
            )
        else:
            if delta_qty < 0:
                raise ValueError("Недостатньо залишків для операції")
            self.db.execute(
                """
                INSERT INTO inventory_balances (
                    id, client_id, warehouse_id, location_id, item_id, batch, serial, expiry_date,
                    qty, reserved_qty, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    generate_uuid(),
                    client_id,
                    warehouse_id,
                    location_id,
                    item_id,
                    batch,
                    serial,
                    expiry_date,
                    delta_qty,
                    0,
                    now_str(),
                ),
            )


class TableFrame(ttk.Frame):
    def __init__(self, master, columns: list[tuple[str, str]], **kwargs):
        super().__init__(master, **kwargs)
        self.tree = ttk.Treeview(self, columns=[c[0] for c in columns], show="headings")
        for col_id, col_name in columns:
            self.tree.heading(col_id, text=col_name)
            self.tree.column(col_id, width=140, anchor="w")
        yscroll = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=yscroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

    def set_rows(self, rows: list[tuple]):
        self.tree.delete(*self.tree.get_children())
        for row in rows:
            self.tree.insert("", "end", values=row)


class WMSApp(tk.Tk):
    def __init__(self, db: DBManager, dal: DAL, service: WMSService, rbac: RBAC):
        super().__init__()
        self.db = db
        self.dal = dal
        self.service = service
        self.rbac = rbac
        self.current_user = self.dal.get_user("admin")
        self.title(APP_TITLE)
        self.geometry("1400x800")
        self.configure(bg="#f2f2f2")
        self.style = ttk.Style(self)
        self.style.theme_use("default")
        self.style.configure("TFrame", background="#f2f2f2")
        self.style.configure("TLabel", background="#f2f2f2")
        self.create_toolbar()
        self.create_layout()
        self.show_dashboard()

    def create_toolbar(self) -> None:
        toolbar = ttk.Frame(self)
        toolbar.pack(side="top", fill="x")
        for label, command in [
            ("Створити", self.not_implemented),
            ("Відкрити", self.not_implemented),
            ("Зберегти", self.not_implemented),
            ("Провести", self.not_implemented),
            ("Друк", self.print_current),
            ("Експорт", self.export_current),
            ("Оновити", self.refresh_current),
            ("Пошук", self.search_current),
        ]:
            btn = ttk.Button(toolbar, text=label, command=command)
            btn.pack(side="left", padx=4, pady=4)

    def create_layout(self) -> None:
        body = ttk.Frame(self)
        body.pack(fill="both", expand=True)
        self.menu_frame = ttk.Frame(body)
        self.menu_frame.pack(side="left", fill="y", padx=4, pady=4)
        self.content_frame = ttk.Frame(body)
        self.content_frame.pack(side="right", fill="both", expand=True)
        self.create_menu()

    def create_menu(self) -> None:
        sections = {
            "Довідники": [
                ("Номенклатура", self.show_items),
                ("Клієнти 3PL", self.show_clients),
                ("Постачальники", self.show_suppliers),
                ("Склади", self.show_warehouses),
                ("Зони", self.show_zones),
                ("Комірки", self.show_locations),
            ],
            "Операції": [
                ("Inbound", self.show_inbound_orders),
                ("Outbound", self.show_outbound_orders),
                ("Інвентаризація", self.show_inventory_counts),
                ("Повернення", self.show_returns),
                ("Списання", self.show_writeoffs),
            ],
            "Звіти": [
                ("Залишки", self.show_inventory_report),
                ("Рух товарів", self.show_moves_report),
                ("Оборотність", self.show_turnover_report),
                ("Термін придатності", self.show_expiry_report),
                ("Звіт по замовленнях", self.show_orders_report),
                ("Робота комірників", self.show_workers_report),
                ("Фінансовий звіт", self.show_fin_report),
            ],
        }
        for section, items in sections.items():
            lbl = ttk.Label(self.menu_frame, text=section, font=("Segoe UI", 10, "bold"))
            lbl.pack(anchor="w", pady=(8, 2))
            for name, cmd in items:
                btn = ttk.Button(self.menu_frame, text=name, width=22, command=cmd)
                btn.pack(anchor="w", pady=1)

    def clear_content(self) -> None:
        for widget in self.content_frame.winfo_children():
            widget.destroy()

    def show_dashboard(self) -> None:
        self.clear_content()
        label = ttk.Label(self.content_frame, text="Головна панель WMS", font=("Segoe UI", 16, "bold"))
        label.pack(pady=20)

    def not_implemented(self) -> None:
        messagebox.showinfo("Інформація", "Функція в розробці.")

    def show_items(self) -> None:
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Номенклатура", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        columns = [
            ("sku", "Артикул"),
            ("name_ua", "Назва"),
            ("client", "Клієнт"),
            ("uom", "Од."),
            ("status", "Статус"),
        ]
        table = TableFrame(self.content_frame, columns)
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["sku"], row["name_ua"], row["client_name"], row["uom_base"], row["status"])
            for row in self.dal.list_items()
        ]
        table.set_rows(rows)
        self.current_view = "items"
        self.current_table = table

    def show_clients(self) -> None:
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Клієнти 3PL", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("code", "Код"), ("name", "Назва"), ("sla", "SLA")])
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["code"], row["name"], row["sla"])
            for row in self.db.execute("SELECT * FROM clients ORDER BY name").fetchall()
        ]
        table.set_rows(rows)
        self.current_view = "clients"
        self.current_table = table

    def show_suppliers(self) -> None:
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Постачальники", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("code", "Код"), ("name", "Назва"), ("contact", "Контакт")])
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["code"], row["name"], row["contact"])
            for row in self.db.execute("SELECT * FROM suppliers ORDER BY name").fetchall()
        ]
        table.set_rows(rows)
        self.current_view = "suppliers"
        self.current_table = table

    def show_warehouses(self) -> None:
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Склади", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("code", "Код"), ("name", "Назва"), ("address", "Адреса")])
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["code"], row["name"], row["address"])
            for row in self.db.execute("SELECT * FROM warehouses ORDER BY name").fetchall()
        ]
        table.set_rows(rows)
        self.current_view = "warehouses"
        self.current_table = table

    def show_zones(self) -> None:
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Зони", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("code", "Код"), ("name", "Назва"), ("type", "Тип")])
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["code"], row["name"], row["zone_type"])
            for row in self.db.execute("SELECT * FROM zones ORDER BY name").fetchall()
        ]
        table.set_rows(rows)
        self.current_view = "zones"
        self.current_table = table

    def show_locations(self) -> None:
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Комірки", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("code", "Код"), ("type", "Тип"), ("zone", "Зона")])
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["code"], row["location_type"], row["zone_id"])
            for row in self.db.execute("SELECT * FROM locations ORDER BY code").fetchall()
        ]
        table.set_rows(rows)
        self.current_view = "locations"
        self.current_table = table

    def show_inbound_orders(self) -> None:
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Inbound", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(
            self.content_frame,
            [("number", "Номер"), ("status", "Статус"), ("client", "Клієнт"), ("date", "Дата")],
        )
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["number"], row["status"], row["client_name"], row["created_at"])
            for row in self.dal.list_inbound_orders()
        ]
        table.set_rows(rows)
        self.current_view = "inbound"
        self.current_table = table

    def show_outbound_orders(self) -> None:
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Outbound", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(
            self.content_frame,
            [("number", "Номер"), ("status", "Статус"), ("client", "Клієнт"), ("date", "Дата")],
        )
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["number"], row["status"], row["client_name"], row["created_at"])
            for row in self.dal.list_outbound_orders()
        ]
        table.set_rows(rows)
        self.current_view = "outbound"
        self.current_table = table

    def show_inventory_counts(self) -> None:
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Інвентаризація", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("number", "Номер"), ("status", "Статус")])
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["number"], row["status"])
            for row in self.db.execute("SELECT * FROM inventory_counts ORDER BY created_at DESC").fetchall()
        ]
        table.set_rows(rows)
        self.current_view = "inventory"
        self.current_table = table

    def show_returns(self) -> None:
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Повернення", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("number", "Номер"), ("status", "Статус")])
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["number"], row["status"])
            for row in self.db.execute("SELECT * FROM returns ORDER BY created_at DESC").fetchall()
        ]
        table.set_rows(rows)
        self.current_view = "returns"
        self.current_table = table

    def show_writeoffs(self) -> None:
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Списання", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("number", "Номер"), ("status", "Статус"), ("reason", "Причина")])
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["number"], row["status"], row["reason"])
            for row in self.db.execute("SELECT * FROM writeoffs ORDER BY created_at DESC").fetchall()
        ]
        table.set_rows(rows)
        self.current_view = "writeoffs"
        self.current_table = table

    def show_inventory_report(self) -> None:
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Залишки", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        columns = [
            ("client", "Клієнт"),
            ("warehouse", "Склад"),
            ("sku", "SKU"),
            ("name", "Назва"),
            ("qty", "К-сть"),
        ]
        table = TableFrame(self.content_frame, columns)
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (
                row["client_name"],
                row["warehouse_name"],
                row["sku"],
                row["name_ua"],
                row["qty"],
            )
            for row in self.dal.list_inventory_balances()
        ]
        table.set_rows(rows)
        self.current_view = "report_inventory"
        self.current_table = table

    def show_moves_report(self) -> None:
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Рух товарів", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(
            self.content_frame,
            [("type", "Тип"), ("doc", "Документ"), ("sku", "SKU"), ("qty", "К-сть"), ("date", "Дата")],
        )
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["move_type"], row["doc_type"], row["item_id"], row["qty"], row["created_at"])
            for row in self.db.execute("SELECT * FROM stock_moves ORDER BY created_at DESC").fetchall()
        ]
        table.set_rows(rows)
        self.current_view = "report_moves"
        self.current_table = table

    def show_turnover_report(self) -> None:
        self.clear_content()
        label = ttk.Label(self.content_frame, text="Оборотність (в розробці)", font=("Segoe UI", 14, "bold"))
        label.pack(pady=20)

    def show_expiry_report(self) -> None:
        self.clear_content()
        label = ttk.Label(self.content_frame, text="Термін придатності (в розробці)", font=("Segoe UI", 14, "bold"))
        label.pack(pady=20)

    def show_orders_report(self) -> None:
        self.clear_content()
        label = ttk.Label(self.content_frame, text="Звіт по замовленнях (в розробці)", font=("Segoe UI", 14, "bold"))
        label.pack(pady=20)

    def show_workers_report(self) -> None:
        self.clear_content()
        label = ttk.Label(self.content_frame, text="Робота комірників (в розробці)", font=("Segoe UI", 14, "bold"))
        label.pack(pady=20)

    def show_fin_report(self) -> None:
        self.clear_content()
        label = ttk.Label(self.content_frame, text="Фінансовий звіт (в розробці)", font=("Segoe UI", 14, "bold"))
        label.pack(pady=20)

    def print_current(self) -> None:
        html_content = """
        <html><head><meta charset='utf-8'></head>
        <body><h2>WMS Друк</h2><p>Документ надруковано з системи WMS.</p></body></html>
        """
        with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp:
            tmp.write(html_content.encode("utf-8"))
            webbrowser.open(tmp.name)

    def export_current(self) -> None:
        if not hasattr(self, "current_table"):
            messagebox.showinfo("Експорт", "Немає даних для експорту")
            return
        file_path = filedialog.asksaveasfilename(defaultextension=".csv")
        if not file_path:
            return
        columns = self.current_table.tree["columns"]
        rows = [self.current_table.tree.item(item)["values"] for item in self.current_table.tree.get_children()]
        with open(file_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            writer.writerows(rows)
        messagebox.showinfo("Експорт", "Експорт виконано")

    def refresh_current(self) -> None:
        view = getattr(self, "current_view", None)
        if view == "items":
            self.show_items()
        elif view == "clients":
            self.show_clients()
        elif view == "suppliers":
            self.show_suppliers()
        elif view == "warehouses":
            self.show_warehouses()
        elif view == "zones":
            self.show_zones()
        elif view == "locations":
            self.show_locations()
        elif view == "inbound":
            self.show_inbound_orders()
        elif view == "outbound":
            self.show_outbound_orders()
        elif view == "inventory":
            self.show_inventory_counts()
        elif view == "returns":
            self.show_returns()
        elif view == "writeoffs":
            self.show_writeoffs()
        elif view == "report_inventory":
            self.show_inventory_report()
        elif view == "report_moves":
            self.show_moves_report()

    def search_current(self) -> None:
        messagebox.showinfo("Пошук", "Фільтри пошуку будуть додані.")


def seed_demo_data(db: DBManager) -> None:
    if db.execute("SELECT COUNT(*) as cnt FROM roles").fetchone()["cnt"] > 0:
        return
    admin_role = generate_uuid()
    roles = [
        (admin_role, "Адміністратор"),
        (generate_uuid(), "Комірник"),
        (generate_uuid(), "Супервайзер складу"),
        (generate_uuid(), "Менеджер логістики"),
        (generate_uuid(), "Клієнт"),
        (generate_uuid(), "Бухгалтер"),
    ]
    db.executemany("INSERT INTO roles (id, name) VALUES (?, ?)", roles)
    permissions = [
        (generate_uuid(), admin_role, "all"),
    ]
    db.executemany(
        "INSERT INTO role_permissions (id, role_id, permission) VALUES (?, ?, ?)",
        permissions,
    )
    salt, pw_hash = hash_password("admin123")
    db.execute(
        """
        INSERT INTO users (id, username, full_name, role_id, salt, password_hash, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (generate_uuid(), "admin", "Адміністратор", admin_role, salt, pw_hash, now_str()),
    )
    client_id = generate_uuid()
    db.execute(
        """
        INSERT INTO clients (id, name, code, contract, sla, billing_type, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (client_id, "ТОВ Клієнт", "CL-001", "Договір-1", "SLA-24h", "операції", now_str()),
    )
    supplier_id = generate_uuid()
    db.execute(
        """
        INSERT INTO suppliers (id, name, code, contact, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (supplier_id, "Постачальник 1", "SUP-001", "+380-00-000", now_str()),
    )
    warehouse_id = generate_uuid()
    db.execute(
        """
        INSERT INTO warehouses (id, name, code, address, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (warehouse_id, "Головний склад", "WH-01", "Київ, вул. Складська 1", now_str()),
    )
    zone_ids = []
    for code, name, ztype in [
        ("PR", "Приймання", "приймання"),
        ("ST", "Зберігання", "зберігання"),
        ("PK", "Пакування", "пакування"),
        ("SH", "Відвантаження", "відвантаження"),
    ]:
        zid = generate_uuid()
        zone_ids.append(zid)
        db.execute(
            """
            INSERT INTO zones (id, warehouse_id, name, code, zone_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (zid, warehouse_id, name, code, ztype, now_str()),
        )
    for idx in range(1, 6):
        db.execute(
            """
            INSERT INTO locations (id, zone_id, code, location_type, capacity_volume, capacity_weight,
                                   capacity_pallets, allowed_category, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                generate_uuid(),
                zone_ids[1],
                f"A-01-0{idx}-01",
                "палета",
                1.0,
                1000.0,
                1,
                None,
                now_str(),
            ),
        )
    items = []
    for idx in range(1, 11):
        item_id = generate_uuid()
        items.append(item_id)
        db.execute(
            """
            INSERT INTO items (
                id, client_id, sku, name_ua, name_en, category, uom_base, uom_alt, weight,
                length, width, height, is_serial, is_batch, has_expiry, temp_mode, storage_rules,
                status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_id,
                client_id,
                f"SKU-{idx:03d}",
                f"Товар {idx}",
                f"Item {idx}",
                "Загальна",
                "шт",
                None,
                1.0,
                10.0,
                10.0,
                10.0,
                0,
                0,
                0,
                "+15..+25",
                "Зберігати сухо",
                "Активний",
                now_str(),
            ),
        )
        db.execute(
            """
            INSERT INTO item_barcodes (id, item_id, barcode, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (generate_uuid(), item_id, f"482000000{idx:03d}", now_str()),
        )
    inbound_id = generate_uuid()
    db.execute(
        """
        INSERT INTO inbound_orders (id, number, status, client_id, supplier_id, warehouse_id,
                                   expected_date, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            inbound_id,
            "IN-0001",
            "Чернетка",
            client_id,
            supplier_id,
            warehouse_id,
            now_str(),
            "admin",
            now_str(),
        ),
    )
    for item_id in items[:3]:
        db.execute(
            """
            INSERT INTO inbound_order_lines (id, order_id, item_id, qty_plan)
            VALUES (?, ?, ?, ?)
            """,
            (generate_uuid(), inbound_id, item_id, 10),
        )
    outbound_id = generate_uuid()
    db.execute(
        """
        INSERT INTO outbound_orders (id, number, status, client_id, warehouse_id, ship_to, carrier,
                                    priority, deadline, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            outbound_id,
            "OUT-0001",
            "Чернетка",
            client_id,
            warehouse_id,
            "Київ, вул. Доставка 1",
            "Нова Пошта",
            "Нормальний",
            now_str(),
            "admin",
            now_str(),
        ),
    )
    for item_id in items[:2]:
        db.execute(
            """
            INSERT INTO outbound_order_lines (id, order_id, item_id, qty_plan)
            VALUES (?, ?, ?, ?)
            """,
            (generate_uuid(), outbound_id, item_id, 5),
        )
    db.commit()


def handle_exception(exc_type, exc, tb):
    logging.error("Unhandled exception: %s", "".join(traceback.format_exception(exc_type, exc, tb)))
    messagebox.showerror("Помилка", f"Непередбачена помилка: {exc}")


def main() -> None:
    tk.Tk.report_callback_exception = staticmethod(handle_exception)
    db = DBManager()
    db.init_schema()
    seed_demo_data(db)
    dal = DAL(db)
    rbac = RBAC(dal)
    rbac.load()
    service = WMSService(db, dal)
    app = WMSApp(db, dal, service, rbac)
    app.mainloop()


if __name__ == "__main__":
    main()
