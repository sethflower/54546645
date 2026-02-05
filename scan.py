import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog
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
DATE_FMT = "%Y-%m-%d"
DATETIME_FMT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def generate_id() -> str:
    timestamp = int(datetime.datetime.now().timestamp() * 1000)
    random_part = str(uuid.uuid4().int % 1_000_000).zfill(6)
    return f"{timestamp}{random_part}"


def generate_sku() -> str:
    return f"ART-{generate_id()}"


def today_str() -> str:
    return datetime.datetime.now().strftime(DATE_FMT)


def now_str() -> str:
    return datetime.datetime.now().strftime(DATETIME_FMT)


def hash_password(password: str, salt: bytes | None = None) -> tuple[str, str]:
    if salt is None:
        salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 100_000)
    return salt.hex(), digest.hex()


def verify_password(password: str, salt_hex: str, digest_hex: str) -> bool:
    salt = bytes.fromhex(salt_hex)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 100_000)
    return digest.hex() == digest_hex


def safe_float(value: str) -> float:
    try:
        return float(value)
    except ValueError:
        return 0.0


def safe_int(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        return 0


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
            supplier_id TEXT,
            brand TEXT,
            sku TEXT NOT NULL,
            name_ua TEXT NOT NULL,
            name_en TEXT,
            category_id TEXT,
            subcategory_id TEXT,
            uom_base TEXT NOT NULL,
            uom_alt TEXT,
            volume REAL NOT NULL DEFAULT 0,
            weight REAL NOT NULL DEFAULT 0,
            length REAL NOT NULL DEFAULT 0,
            width REAL NOT NULL DEFAULT 0,
            height REAL NOT NULL DEFAULT 0,
            is_serial INTEGER NOT NULL DEFAULT 0,
            is_batch INTEGER NOT NULL DEFAULT 0,
            has_expiry INTEGER NOT NULL DEFAULT 0,
            temp_mode TEXT,
            storage_rules TEXT,
            barcode TEXT,
            product TEXT,
            status TEXT NOT NULL DEFAULT 'Активний',
            created_at TEXT NOT NULL,
            UNIQUE(client_id, sku),
            FOREIGN KEY(client_id) REFERENCES clients(id),
            FOREIGN KEY(supplier_id) REFERENCES suppliers(id),
            FOREIGN KEY(category_id) REFERENCES categories(id),
            FOREIGN KEY(subcategory_id) REFERENCES subcategories(id)
        );
        CREATE TABLE IF NOT EXISTS categories (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS subcategories (
            id TEXT PRIMARY KEY,
            category_id TEXT NOT NULL,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(category_id, name),
            FOREIGN KEY(category_id) REFERENCES categories(id)
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
        self.ensure_columns()
        self.connection.commit()

    def ensure_columns(self) -> None:
        columns = {row["name"] for row in self.execute("PRAGMA table_info(items)").fetchall()}
        additions = [
            ("supplier_id", "TEXT"),
            ("brand", "TEXT"),
            ("category_id", "TEXT"),
            ("subcategory_id", "TEXT"),
            ("volume", "REAL NOT NULL DEFAULT 0"),
            ("barcode", "TEXT"),
            ("product", "TEXT"),
        ]
        for name, col_type in additions:
            if name not in columns:
                self.execute(f"ALTER TABLE items ADD COLUMN {name} {col_type}")


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

    def update_user_login_failure(self, user_id: str, attempts: int, locked_until: str | None) -> None:
        self.db.execute(
            "UPDATE users SET failed_attempts = ?, locked_until = ? WHERE id = ?",
            (attempts, locked_until, user_id),
        )
        self.db.commit()

    def reset_user_lock(self, user_id: str) -> None:
        self.db.execute(
            "UPDATE users SET failed_attempts = 0, locked_until = NULL WHERE id = ?",
            (user_id,),
        )
        self.db.commit()

    def list_items(self, client_id: str | None = None) -> list[sqlite3.Row]:
        if client_id:
            return self.db.execute(
                """
                SELECT items.*, clients.name AS client_name, suppliers.name AS supplier_name,
                       categories.name AS category_name, subcategories.name AS subcategory_name
                FROM items
                JOIN clients ON clients.id = items.client_id
                LEFT JOIN suppliers ON suppliers.id = items.supplier_id
                LEFT JOIN categories ON categories.id = items.category_id
                LEFT JOIN subcategories ON subcategories.id = items.subcategory_id
                WHERE items.client_id = ?
                ORDER BY items.sku
                """,
                (client_id,),
            ).fetchall()
        return self.db.execute(
            """
            SELECT items.*, clients.name AS client_name, suppliers.name AS supplier_name,
                   categories.name AS category_name, subcategories.name AS subcategory_name
            FROM items
            JOIN clients ON clients.id = items.client_id
            LEFT JOIN suppliers ON suppliers.id = items.supplier_id
            LEFT JOIN categories ON categories.id = items.category_id
            LEFT JOIN subcategories ON subcategories.id = items.subcategory_id
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
            (generate_id(), user_id, action, entity, entity_id, description, now_str()),
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
        return "all" in self.permissions.get(role_id, set()) or permission in self.permissions.get(role_id, set())


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
        move_id = generate_id()
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
                    generate_id(),
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


class LoginDialog(tk.Toplevel):
    def __init__(self, master: tk.Tk, dal: DAL):
        super().__init__(master)
        self.dal = dal
        self.result: sqlite3.Row | None = None
        self.title("Вхід до системи")
        self.geometry("360x200")
        self.resizable(False, False)
        self.protocol("WM_DELETE_WINDOW", self.on_close)

        frame = ttk.Frame(self, padding=16)
        frame.pack(fill="both", expand=True)

        ttk.Label(frame, text="Логін:").grid(row=0, column=0, sticky="w")
        self.username_entry = ttk.Entry(frame)
        self.username_entry.grid(row=0, column=1, sticky="ew", pady=4)

        ttk.Label(frame, text="Пароль:").grid(row=1, column=0, sticky="w")
        self.password_entry = ttk.Entry(frame, show="*")
        self.password_entry.grid(row=1, column=1, sticky="ew", pady=4)

        self.message_label = ttk.Label(frame, text="")
        self.message_label.grid(row=2, column=0, columnspan=2, sticky="w", pady=4)

        btn = ttk.Button(frame, text="Увійти", command=self.attempt_login)
        btn.grid(row=3, column=0, columnspan=2, pady=8)

        frame.columnconfigure(1, weight=1)
        self.username_entry.focus()

    def on_close(self) -> None:
        self.result = None
        self.destroy()

    def attempt_login(self) -> None:
        username = self.username_entry.get().strip()
        password = self.password_entry.get().strip()
        user = self.dal.get_user(username)
        if not user:
            self.message_label.config(text="Невірний логін або пароль")
            return
        if not user["is_active"]:
            self.message_label.config(text="Користувач заблокований")
            return
        locked_until = user["locked_until"]
        if locked_until:
            locked_dt = datetime.datetime.strptime(locked_until, DATETIME_FMT)
            if locked_dt > datetime.datetime.now():
                self.message_label.config(text=f"Блокування до {locked_until}")
                return
        if verify_password(password, user["salt"], user["password_hash"]):
            self.dal.reset_user_lock(user["id"])
            self.result = user
            self.destroy()
            return
        attempts = user["failed_attempts"] + 1
        if attempts >= 5:
            locked_until = (datetime.datetime.now() + datetime.timedelta(minutes=15)).strftime(DATETIME_FMT)
        else:
            locked_until = None
        self.dal.update_user_login_failure(user["id"], attempts, locked_until)
        self.message_label.config(text="Невірний логін або пароль")


class TableFrame(ttk.Frame):
    def __init__(self, master, columns: list[tuple[str, str]], **kwargs):
        super().__init__(master, **kwargs)
        self.tree = ttk.Treeview(self, columns=[c[0] for c in columns], show="headings", selectmode="browse")
        for col_id, col_name in columns:
            self.tree.heading(col_id, text=col_name)
            self.tree.column(col_id, width=140, anchor="w")
        yscroll = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=yscroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        self.tree.bind("<Control-c>", self.copy_selected)
        self.menu = tk.Menu(self, tearoff=0)
        self.menu.add_command(label="Копіювати", command=self.copy_selected)
        self.tree.bind("<Button-3>", self.show_menu)

    def set_rows(self, rows: list[tuple]):
        self.tree.delete(*self.tree.get_children())
        for row in rows:
            self.tree.insert("", "end", values=row)

    def selected_values(self) -> tuple | None:
        selected = self.tree.selection()
        if not selected:
            return None
        return self.tree.item(selected[0])["values"]

    def copy_selected(self, _event=None) -> None:
        values = self.selected_values()
        if not values:
            return
        text = "\t".join(str(v) for v in values)
        self.clipboard_clear()
        self.clipboard_append(text)

    def show_menu(self, event: tk.Event) -> None:
        self.menu.tk_popup(event.x_root, event.y_root)


class WMSApp(tk.Tk):
    def __init__(self, db: DBManager, dal: DAL, service: WMSService, rbac: RBAC, user: sqlite3.Row):
        super().__init__()
        self.db = db
        self.dal = dal
        self.service = service
        self.rbac = rbac
        self.current_user = user
        self.title(APP_TITLE)
        self.geometry("1500x850")
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
            ("Створити", self.create_current),
            ("Відкрити", self.edit_current),
            ("Зберегти", self.save_current),
            ("Провести", self.post_current),
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
                ("Користувачі", self.show_users),
                ("Ролі", self.show_roles),
            ],
            "Операції": [
                ("Замовлення на приймання", self.show_inbound_orders),
                ("Фактична приймання", self.show_receipts),
                ("Putaway", self.show_putaway),
                ("Замовлення клієнта", self.show_outbound_orders),
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
                ("Аудит", self.show_audit_report),
            ],
        }
        for section, items in sections.items():
            lbl = ttk.Label(self.menu_frame, text=section, font=("Segoe UI", 10, "bold"))
            lbl.pack(anchor="w", pady=(8, 2))
            for name, cmd in items:
                btn = ttk.Button(self.menu_frame, text=name, width=26, command=cmd)
                btn.pack(anchor="w", pady=1)

    def clear_content(self) -> None:
        for widget in self.content_frame.winfo_children():
            widget.destroy()

    def show_dashboard(self) -> None:
        self.clear_content()
        label = ttk.Label(self.content_frame, text="Головна панель WMS", font=("Segoe UI", 16, "bold"))
        label.pack(pady=20)

    def set_list_buttons(self, parent: ttk.Frame, handlers: dict[str, callable]) -> None:
        btn_frame = ttk.Frame(parent)
        btn_frame.pack(fill="x", padx=8, pady=4)
        for label, handler in handlers.items():
            ttk.Button(btn_frame, text=label, command=handler).pack(side="left", padx=4)

    def has_perm(self, perm: str) -> bool:
        return self.rbac.has_permission(self.current_user["role_id"], perm)

    def ensure_perm(self, perm: str) -> bool:
        if not self.has_perm(perm):
            messagebox.showerror("Доступ заборонено", "Недостатньо прав")
            return False
        return True

    def show_items(self) -> None:
        if not self.ensure_perm("items.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Номенклатура", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        search_frame = ttk.Frame(self.content_frame)
        search_frame.pack(fill="x", padx=8)
        ttk.Label(search_frame, text="Пошук по марці:").pack(side="left")
        search_entry = ttk.Entry(search_frame)
        search_entry.pack(side="left", fill="x", expand=True, padx=4)
        columns = [
            ("sku", "Артикул"),
            ("brand", "Марка"),
            ("supplier", "Постачальник"),
            ("client", "3PL клієнт"),
            ("uom", "Од."),
            ("volume", "Об'єм"),
            ("weight", "Вага"),
            ("barcode", "Штрих-код"),
            ("serial", "Серійний облік"),
            ("category", "Категорія"),
            ("subcategory", "Підкатегорія"),
            ("product", "Продакт"),
        ]
        table = TableFrame(self.content_frame, columns)
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            data = self.dal.list_items()
            query = search_entry.get().strip().lower()
            if query:
                data = [row for row in data if (row["brand"] or "").lower().find(query) >= 0]
            rows = [
                (
                    row["sku"],
                    row["brand"],
                    row["supplier_name"],
                    row["client_name"],
                    row["uom_base"],
                    row["volume"],
                    row["weight"],
                    row["barcode"],
                    "Так" if row["is_serial"] else "Ні",
                    row["category_name"],
                    row["subcategory_name"],
                    row["product"],
                )
                for row in data
            ]
            table.set_rows(rows)

        search_entry.bind("<KeyRelease>", lambda _event: load())
        load()
        self.current_view = "items"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_item,
                "Редагувати": self.edit_item,
                "Видалити": self.delete_item,
                "Оновити": load,
            },
        )

    def show_clients(self) -> None:
        if not self.ensure_perm("clients.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Клієнти 3PL", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        columns = [("id", "ID"), ("code", "Код"), ("name", "Назва"), ("sla", "SLA")]
        table = TableFrame(self.content_frame, columns)
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            rows = [
                (row["id"], row["code"], row["name"], row["sla"])
                for row in self.db.execute("SELECT * FROM clients ORDER BY name").fetchall()
            ]
            table.set_rows(rows)

        load()
        self.current_view = "clients"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_client,
                "Редагувати": self.edit_client,
                "Видалити": self.delete_client,
                "Оновити": load,
            },
        )

    def show_suppliers(self) -> None:
        if not self.ensure_perm("suppliers.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Постачальники", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("id", "ID"), ("code", "Код"), ("name", "Назва")])
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            rows = [
                (row["id"], row["code"], row["name"])
                for row in self.db.execute("SELECT * FROM suppliers ORDER BY name").fetchall()
            ]
            table.set_rows(rows)

        load()
        self.current_view = "suppliers"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_supplier,
                "Редагувати": self.edit_supplier,
                "Видалити": self.delete_supplier,
                "Оновити": load,
            },
        )

    def show_warehouses(self) -> None:
        if not self.ensure_perm("warehouses.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Склади", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("id", "ID"), ("code", "Код"), ("name", "Назва"), ("address", "Адреса")])
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            rows = [
                (row["id"], row["code"], row["name"], row["address"])
                for row in self.db.execute("SELECT * FROM warehouses ORDER BY name").fetchall()
            ]
            table.set_rows(rows)

        load()
        self.current_view = "warehouses"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_warehouse,
                "Редагувати": self.edit_warehouse,
                "Видалити": self.delete_warehouse,
                "Оновити": load,
            },
        )

    def show_zones(self) -> None:
        if not self.ensure_perm("zones.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Зони", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("id", "ID"), ("code", "Код"), ("name", "Назва"), ("type", "Тип")])
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            rows = [
                (row["id"], row["code"], row["name"], row["zone_type"])
                for row in self.db.execute("SELECT * FROM zones ORDER BY name").fetchall()
            ]
            table.set_rows(rows)

        load()
        self.current_view = "zones"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_zone,
                "Редагувати": self.edit_zone,
                "Видалити": self.delete_zone,
                "Оновити": load,
            },
        )

    def show_locations(self) -> None:
        if not self.ensure_perm("locations.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Комірки", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("id", "ID"), ("code", "Код"), ("type", "Тип"), ("zone", "Зона")])
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            rows = [
                (row["id"], row["code"], row["location_type"], row["zone_id"])
                for row in self.db.execute("SELECT * FROM locations ORDER BY code").fetchall()
            ]
            table.set_rows(rows)

        load()
        self.current_view = "locations"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_location,
                "Редагувати": self.edit_location,
                "Видалити": self.delete_location,
                "Оновити": load,
            },
        )

    def show_users(self) -> None:
        if not self.ensure_perm("users.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Користувачі", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("id", "ID"), ("username", "Логін"), ("full_name", "ПІБ"), ("role", "Роль")])
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            rows = [
                (row["id"], row["username"], row["full_name"], row["role_id"])
                for row in self.db.execute("SELECT * FROM users ORDER BY username").fetchall()
            ]
            table.set_rows(rows)

        load()
        self.current_view = "users"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_user,
                "Редагувати": self.edit_user,
                "Видалити": self.delete_user,
                "Оновити": load,
            },
        )

    def show_roles(self) -> None:
        if not self.ensure_perm("roles.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Ролі та права", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("id", "ID"), ("name", "Назва")])
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            rows = [
                (row["id"], row["name"])
                for row in self.db.execute("SELECT * FROM roles ORDER BY name").fetchall()
            ]
            table.set_rows(rows)

        load()
        self.current_view = "roles"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_role,
                "Редагувати": self.edit_role,
                "Видалити": self.delete_role,
                "Оновити": load,
            },
        )

    def show_inbound_orders(self) -> None:
        if not self.ensure_perm("inbound.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Замовлення на приймання", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(
            self.content_frame,
            [("id", "ID"), ("number", "Номер"), ("status", "Статус"), ("client", "Клієнт"), ("date", "Дата")],
        )
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            rows = [
                (row["id"], row["number"], row["status"], row["client_name"], row["created_at"])
                for row in self.dal.list_inbound_orders()
            ]
            table.set_rows(rows)

        load()
        self.current_view = "inbound"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_inbound,
                "Редагувати": self.edit_inbound,
                "Видалити": self.delete_inbound,
                "Провести": self.post_inbound,
                "Оновити": load,
            },
        )

    def show_receipts(self) -> None:
        if not self.ensure_perm("receipts.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Фактична приймання", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(
            self.content_frame,
            [("id", "ID"), ("number", "Номер"), ("status", "Статус"), ("date", "Дата")],
        )
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            rows = [
                (row["id"], row["number"], row["status"], row["received_at"])
                for row in self.db.execute("SELECT * FROM receipts ORDER BY received_at DESC").fetchall()
            ]
            table.set_rows(rows)

        load()
        self.current_view = "receipts"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_receipt,
                "Редагувати": self.edit_receipt,
                "Видалити": self.delete_receipt,
                "Провести": self.post_receipt,
                "Оновити": load,
            },
        )

    def show_putaway(self) -> None:
        if not self.ensure_perm("putaway.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Putaway задачі", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("id", "ID"), ("status", "Статус"), ("qty", "К-сть")])
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            rows = [
                (row["id"], row["status"], row["qty"])
                for row in self.db.execute("SELECT * FROM putaway_tasks ORDER BY created_at DESC").fetchall()
            ]
            table.set_rows(rows)

        load()
        self.current_view = "putaway"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_putaway,
                "Редагувати": self.edit_putaway,
                "Видалити": self.delete_putaway,
                "Оновити": load,
            },
        )

    def show_outbound_orders(self) -> None:
        if not self.ensure_perm("outbound.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Замовлення клієнта", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(
            self.content_frame,
            [("id", "ID"), ("number", "Номер"), ("status", "Статус"), ("client", "Клієнт"), ("date", "Дата")],
        )
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            rows = [
                (row["id"], row["number"], row["status"], row["client_name"], row["created_at"])
                for row in self.dal.list_outbound_orders()
            ]
            table.set_rows(rows)

        load()
        self.current_view = "outbound"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_outbound,
                "Редагувати": self.edit_outbound,
                "Видалити": self.delete_outbound,
                "Провести": self.post_outbound,
                "Оновити": load,
            },
        )

    def show_inventory_counts(self) -> None:
        if not self.ensure_perm("inventory.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Інвентаризація", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("id", "ID"), ("number", "Номер"), ("status", "Статус")])
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            rows = [
                (row["id"], row["number"], row["status"])
                for row in self.db.execute("SELECT * FROM inventory_counts ORDER BY created_at DESC").fetchall()
            ]
            table.set_rows(rows)

        load()
        self.current_view = "inventory"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_inventory_count,
                "Редагувати": self.edit_inventory_count,
                "Видалити": self.delete_inventory_count,
                "Провести": self.post_inventory_count,
                "Оновити": load,
            },
        )

    def show_returns(self) -> None:
        if not self.ensure_perm("returns.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Повернення", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("id", "ID"), ("number", "Номер"), ("status", "Статус")])
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            rows = [
                (row["id"], row["number"], row["status"])
                for row in self.db.execute("SELECT * FROM returns ORDER BY created_at DESC").fetchall()
            ]
            table.set_rows(rows)

        load()
        self.current_view = "returns"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_return,
                "Редагувати": self.edit_return,
                "Видалити": self.delete_return,
                "Провести": self.post_return,
                "Оновити": load,
            },
        )

    def show_writeoffs(self) -> None:
        if not self.ensure_perm("writeoffs.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Списання", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("id", "ID"), ("number", "Номер"), ("status", "Статус"), ("reason", "Причина")])
        table.pack(fill="both", expand=True, padx=8, pady=4)

        def load():
            rows = [
                (row["id"], row["number"], row["status"], row["reason"])
                for row in self.db.execute("SELECT * FROM writeoffs ORDER BY created_at DESC").fetchall()
            ]
            table.set_rows(rows)

        load()
        self.current_view = "writeoffs"
        self.current_table = table
        self.set_list_buttons(
            self.content_frame,
            {
                "Створити": self.create_writeoff,
                "Редагувати": self.edit_writeoff,
                "Видалити": self.delete_writeoff,
                "Провести": self.post_writeoff,
                "Оновити": load,
            },
        )

    def show_inventory_report(self) -> None:
        if not self.ensure_perm("reports.view"):
            return
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
        if not self.ensure_perm("reports.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Рух товарів", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(
            self.content_frame,
            [("type", "Тип"), ("doc", "Документ"), ("item", "Номенклатура"), ("qty", "К-сть"), ("date", "Дата")],
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
        if not self.ensure_perm("reports.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Оборотність", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("sku", "SKU"), ("moves", "К-сть рухів")])
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["item_id"], row["cnt"])
            for row in self.db.execute(
                "SELECT item_id, COUNT(*) as cnt FROM stock_moves GROUP BY item_id ORDER BY cnt DESC"
            ).fetchall()
        ]
        table.set_rows(rows)
        self.current_view = "report_turnover"
        self.current_table = table

    def show_expiry_report(self) -> None:
        if not self.ensure_perm("reports.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Термін придатності", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("sku", "SKU"), ("expiry", "Термін"), ("qty", "К-сть")])
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["sku"], row["expiry_date"], row["qty"])
            for row in self.db.execute(
                """
                SELECT items.sku, inventory_balances.expiry_date, inventory_balances.qty
                FROM inventory_balances
                JOIN items ON items.id = inventory_balances.item_id
                WHERE inventory_balances.expiry_date IS NOT NULL
                ORDER BY inventory_balances.expiry_date
                """
            ).fetchall()
        ]
        table.set_rows(rows)
        self.current_view = "report_expiry"
        self.current_table = table

    def show_orders_report(self) -> None:
        if not self.ensure_perm("reports.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Звіт по замовленнях", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(
            self.content_frame,
            [("number", "Номер"), ("status", "Статус"), ("client", "Клієнт"), ("deadline", "Дедлайн")],
        )
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["number"], row["status"], row["client_name"], row["deadline"])
            for row in self.dal.list_outbound_orders()
        ]
        table.set_rows(rows)
        self.current_view = "report_orders"
        self.current_table = table

    def show_workers_report(self) -> None:
        if not self.ensure_perm("reports.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Робота комірників", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("user", "Користувач"), ("moves", "Операцій")])
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["created_by"], row["cnt"])
            for row in self.db.execute(
                "SELECT created_by, COUNT(*) as cnt FROM stock_moves GROUP BY created_by ORDER BY cnt DESC"
            ).fetchall()
        ]
        table.set_rows(rows)
        self.current_view = "report_workers"
        self.current_table = table

    def show_fin_report(self) -> None:
        if not self.ensure_perm("reports.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Фінансовий звіт", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(self.content_frame, [("client", "Клієнт"), ("moves", "Операції")])
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["client_id"], row["cnt"])
            for row in self.db.execute(
                "SELECT client_id, COUNT(*) as cnt FROM stock_moves GROUP BY client_id ORDER BY cnt DESC"
            ).fetchall()
        ]
        table.set_rows(rows)
        self.current_view = "report_fin"
        self.current_table = table

    def show_audit_report(self) -> None:
        if not self.ensure_perm("reports.view"):
            return
        self.clear_content()
        header = ttk.Label(self.content_frame, text="Аудит", font=("Segoe UI", 14, "bold"))
        header.pack(anchor="w", padx=8, pady=8)
        table = TableFrame(
            self.content_frame,
            [("user", "Користувач"), ("action", "Дія"), ("entity", "Сутність"), ("date", "Дата")],
        )
        table.pack(fill="both", expand=True, padx=8, pady=4)
        rows = [
            (row["user_id"], row["action"], row["entity"], row["created_at"])
            for row in self.db.execute("SELECT * FROM audit_log ORDER BY created_at DESC").fetchall()
        ]
        table.set_rows(rows)
        self.current_view = "report_audit"
        self.current_table = table

    def get_selected_id(self) -> str | None:
        if not hasattr(self, "current_table"):
            return None
        values = self.current_table.selected_values()
        if not values:
            return None
        return values[0]

    def create_current(self) -> None:
        view_map = {
            "items": self.create_item,
            "clients": self.create_client,
            "suppliers": self.create_supplier,
            "warehouses": self.create_warehouse,
            "zones": self.create_zone,
            "locations": self.create_location,
            "users": self.create_user,
            "roles": self.create_role,
            "inbound": self.create_inbound,
            "receipts": self.create_receipt,
            "outbound": self.create_outbound,
            "inventory": self.create_inventory_count,
            "returns": self.create_return,
            "writeoffs": self.create_writeoff,
        }
        handler = view_map.get(getattr(self, "current_view", ""))
        if handler:
            handler()

    def edit_current(self) -> None:
        view_map = {
            "items": self.edit_item,
            "clients": self.edit_client,
            "suppliers": self.edit_supplier,
            "warehouses": self.edit_warehouse,
            "zones": self.edit_zone,
            "locations": self.edit_location,
            "users": self.edit_user,
            "roles": self.edit_role,
            "inbound": self.edit_inbound,
            "receipts": self.edit_receipt,
            "outbound": self.edit_outbound,
            "inventory": self.edit_inventory_count,
            "returns": self.edit_return,
            "writeoffs": self.edit_writeoff,
        }
        handler = view_map.get(getattr(self, "current_view", ""))
        if handler:
            handler()

    def save_current(self) -> None:
        messagebox.showinfo("Збереження", "Дані збережено через форми.")

    def post_current(self) -> None:
        view_map = {
            "inbound": self.post_inbound,
            "receipts": self.post_receipt,
            "outbound": self.post_outbound,
            "inventory": self.post_inventory_count,
            "returns": self.post_return,
            "writeoffs": self.post_writeoff,
        }
        handler = view_map.get(getattr(self, "current_view", ""))
        if handler:
            handler()

    def print_current(self) -> None:
        view = getattr(self, "current_view", "")
        selected_id = self.get_selected_id()
        html = "<html><head><meta charset='utf-8'></head><body>"
        html += f"<h2>Друк: {view}</h2>"
        if selected_id:
            html += f"<p>ID документа: {selected_id}</p>"
        html += "<p>Форма друку сформована WMS.</p></body></html>"
        with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp:
            tmp.write(html.encode("utf-8"))
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
        elif view == "users":
            self.show_users()
        elif view == "roles":
            self.show_roles()
        elif view == "inbound":
            self.show_inbound_orders()
        elif view == "receipts":
            self.show_receipts()
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
        elif view == "report_turnover":
            self.show_turnover_report()
        elif view == "report_expiry":
            self.show_expiry_report()
        elif view == "report_orders":
            self.show_orders_report()
        elif view == "report_workers":
            self.show_workers_report()
        elif view == "report_fin":
            self.show_fin_report()
        elif view == "report_audit":
            self.show_audit_report()

    def search_current(self) -> None:
        messagebox.showinfo("Пошук", "Скористайтеся полем пошуку у формі.")

    def create_simple_form(self, title: str, fields: list[tuple[str, str]], initial: dict[str, str] | None = None) -> dict[str, tk.StringVar]:
        window = tk.Toplevel(self)
        window.title(title)
        window.geometry("480x400")
        frame = ttk.Frame(window, padding=12)
        frame.pack(fill="both", expand=True)
        vars_map: dict[str, tk.StringVar] = {}
        menu = tk.Menu(window, tearoff=0)
        menu.add_command(label="Копіювати", command=lambda: window.focus_get().event_generate("<<Copy>>"))
        menu.add_command(label="Вставити", command=lambda: window.focus_get().event_generate("<<Paste>>"))
        menu.add_command(label="Вирізати", command=lambda: window.focus_get().event_generate("<<Cut>>"))

        def show_menu(event: tk.Event) -> None:
            menu.tk_popup(event.x_root, event.y_root)

        def digits_only(value: str) -> bool:
            return value.isdigit() or value == ""

        vcmd = (window.register(digits_only), "%P")
        lookup_map = {
            "client_id": ("clients", "name"),
            "supplier_id": ("suppliers", "name"),
            "warehouse_id": ("warehouses", "name"),
            "zone_id": ("zones", "name"),
            "location_id": ("locations", "code"),
            "item_id": ("items", "sku"),
            "role_id": ("roles", "name"),
            "order_id": ("inbound_orders", "number"),
            "category_id": ("categories", "name"),
            "subcategory_id": ("subcategories", "name"),
        }
        for idx, (key, label) in enumerate(fields):
            ttk.Label(frame, text=label).grid(row=idx, column=0, sticky="w", pady=4)
            var = tk.StringVar(value=(initial.get(key) if initial else ""))
            entry = ttk.Entry(frame, textvariable=var)
            if key.endswith("_id"):
                entry.configure(validate="key", validatecommand=vcmd)
            entry.grid(row=idx, column=1, sticky="ew", pady=4)
            entry.bind("<Button-3>", show_menu)
            vars_map[key] = var
            if key in lookup_map:
                table, display = lookup_map[key]
                entry.configure(state="readonly")
                ttk.Button(
                    frame,
                    text="...",
                    command=lambda v=var, t=table, d=display: self.open_lookup(v, t, d),
                ).grid(row=idx, column=2, padx=4)
        frame.columnconfigure(1, weight=1)
        btn = ttk.Button(frame, text="Зберегти", command=window.destroy)
        btn.grid(row=len(fields), column=0, columnspan=2, pady=8)
        window.grab_set()
        self.wait_window(window)
        return vars_map

    def open_lookup(
        self,
        target_var: tk.StringVar,
        table: str,
        display_field: str,
        category_var: tk.StringVar | None = None,
    ) -> None:
        window = tk.Toplevel(self)
        window.title("Вибір значення")
        window.geometry("500x400")
        frame = ttk.Frame(window, padding=10)
        frame.pack(fill="both", expand=True)
        search_frame = ttk.Frame(frame)
        search_frame.pack(fill="x", pady=4)
        ttk.Label(search_frame, text="Пошук:").pack(side="left")
        search_var = tk.StringVar()
        search_entry = ttk.Entry(search_frame, textvariable=search_var)
        search_entry.pack(side="left", fill="x", expand=True, padx=4)
        table_frame = TableFrame(frame, [("id", "ID"), ("display", "Назва")])
        table_frame.pack(fill="both", expand=True)

        def load_rows() -> None:
            query = search_var.get().strip()
            if query:
                rows = self.db.execute(
                    f"SELECT id, {display_field} FROM {table} WHERE {display_field} LIKE ? ORDER BY {display_field}",
                    (f"%{query}%",),
                ).fetchall()
            else:
                rows = self.db.execute(
                    f"SELECT id, {display_field} FROM {table} ORDER BY {display_field}"
                ).fetchall()
            table_frame.set_rows([(row["id"], row[display_field]) for row in rows])

        def choose():
            selected = table_frame.selected_values()
            if not selected:
                return
            target_var.set(str(selected[0]))
            window.destroy()

        def create_new() -> None:
            if table == "categories":
                name = simpledialog.askstring("Нова категорія", "Назва категорії:", parent=window)
                if not name:
                    return
                self.db.execute(
                    "INSERT INTO categories (id, name, created_at) VALUES (?, ?, ?)",
                    (generate_id(), name, now_str()),
                )
                self.db.commit()
                load_rows()
                return
            if table == "subcategories":
                name = simpledialog.askstring("Нова підкатегорія", "Назва підкатегорії:", parent=window)
                if not name:
                    return
                category_id = category_var.get() if category_var and category_var.get() else ""
                if not category_id:
                    category_id = self.select_lookup_value("categories", "name")
                if not category_id:
                    messagebox.showerror("Помилка", "Оберіть категорію")
                    return
                self.db.execute(
                    "INSERT INTO subcategories (id, category_id, name, created_at) VALUES (?, ?, ?, ?)",
                    (generate_id(), category_id, name, now_str()),
                )
                self.db.commit()
                load_rows()

        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill="x", pady=4)
        ttk.Button(btn_frame, text="Обрати", command=choose).pack(side="left", padx=4)
        if table in {"categories", "subcategories"}:
            ttk.Button(btn_frame, text="Створити", command=create_new).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Закрити", command=window.destroy).pack(side="right")
        search_entry.bind("<KeyRelease>", lambda _event: load_rows())
        load_rows()
        search_entry.focus()
        window.grab_set()
        self.wait_window(window)

    def select_lookup_value(self, table: str, display_field: str) -> str:
        selected_value = tk.StringVar()
        self.open_lookup(selected_value, table, display_field)
        return selected_value.get()

    def get_item_client(self, item_id: str) -> str:
        row = self.db.execute("SELECT client_id FROM items WHERE id = ?", (item_id,)).fetchone()
        return row["client_id"] if row else ""

    def create_item(self) -> None:
        if not self.ensure_perm("items.edit"):
            return
        clients = self.db.execute("SELECT id, name FROM clients ORDER BY name").fetchall()
        if not clients:
            messagebox.showerror("Помилка", "Спочатку створіть клієнта")
            return
        sku = generate_sku()
        window = tk.Toplevel(self)
        window.title("Створити номенклатуру")
        window.geometry("520x520")
        frame = ttk.Frame(window, padding=12)
        frame.pack(fill="both", expand=True)

        fields = [
            ("brand", "Марка"),
            ("sku", "Артикул"),
            ("supplier_id", "Постачальник"),
            ("client_id", "3PL клієнт"),
            ("uom_base", "Од. виміру"),
            ("volume", "Об'єм"),
            ("weight", "Вага"),
            ("barcode", "Штрихкод"),
            ("is_serial", "Серійний облік"),
            ("category_id", "Категорія"),
            ("subcategory_id", "Підкатегорія"),
            ("product", "Продакт"),
        ]
        vars_map: dict[str, tk.StringVar] = {}
        for idx, (key, label) in enumerate(fields):
            ttk.Label(frame, text=label).grid(row=idx, column=0, sticky="w", pady=4)
            var = tk.StringVar()
            entry = ttk.Entry(frame, textvariable=var)
            if key == "sku":
                var.set(sku)
                entry.configure(state="readonly")
            if key in {"supplier_id", "client_id", "category_id", "subcategory_id"}:
                entry.configure(state="readonly")
                lookup_map = {
                    "supplier_id": ("suppliers", "name"),
                    "client_id": ("clients", "name"),
                    "category_id": ("categories", "name"),
                    "subcategory_id": ("subcategories", "name"),
                }
                table, display = lookup_map[key]
                category_var = vars_map.get("category_id") if key == "subcategory_id" else None
                ttk.Button(
                    frame,
                    text="...",
                    command=lambda v=var, t=table, d=display, c=category_var: self.open_lookup(v, t, d, c),
                ).grid(row=idx, column=2, padx=4)
            if key == "uom_base":
                entry.destroy()
                entry = ttk.Combobox(frame, textvariable=var, values=["шт", "палета"], state="readonly")
            if key == "is_serial":
                entry.destroy()
                entry = ttk.Combobox(frame, textvariable=var, values=["Так", "Ні"], state="readonly")
            if key == "product":
                var.set(self.current_user["username"])
                entry.configure(state="readonly")
            entry.grid(row=idx, column=1, sticky="ew", pady=4)
            vars_map[key] = var

        def save():
            self.db.execute(
                """
                INSERT INTO items (
                    id, client_id, supplier_id, brand, sku, name_ua, category_id, subcategory_id,
                    uom_base, volume, weight, barcode, is_serial, product, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    generate_id(),
                    vars_map["client_id"].get(),
                    vars_map["supplier_id"].get(),
                    vars_map["brand"].get(),
                    vars_map["sku"].get(),
                    vars_map["brand"].get() or vars_map["sku"].get(),
                    vars_map["category_id"].get(),
                    vars_map["subcategory_id"].get(),
                    vars_map["uom_base"].get() or "шт",
                    safe_float(vars_map["volume"].get()),
                    safe_float(vars_map["weight"].get()),
                    vars_map["barcode"].get(),
                    1 if vars_map["is_serial"].get() == "Так" else 0,
                    vars_map["product"].get(),
                    "Активний",
                    now_str(),
                ),
            )
            self.db.commit()
            self.dal.insert_audit(self.current_user["id"], "create", "items", vars_map["sku"].get(), "Створено")
            window.destroy()
            self.refresh_current()

        frame.columnconfigure(1, weight=1)
        ttk.Button(frame, text="Зберегти", command=save).grid(row=len(fields), column=0, columnspan=2, pady=8)
        window.grab_set()
        self.wait_window(window)

    def edit_item(self) -> None:
        if not self.ensure_perm("items.edit"):
            return
        item_id = self.get_selected_id()
        if not item_id:
            messagebox.showerror("Помилка", "Оберіть запис")
            return
        item = self.db.execute("SELECT * FROM items WHERE id = ?", (item_id,)).fetchone()
        if not item:
            return
        window = tk.Toplevel(self)
        window.title("Редагувати номенклатуру")
        window.geometry("520x520")
        frame = ttk.Frame(window, padding=12)
        frame.pack(fill="both", expand=True)
        fields = [
            ("brand", "Марка"),
            ("sku", "Артикул"),
            ("supplier_id", "Постачальник"),
            ("client_id", "3PL клієнт"),
            ("uom_base", "Од. виміру"),
            ("volume", "Об'єм"),
            ("weight", "Вага"),
            ("barcode", "Штрихкод"),
            ("is_serial", "Серійний облік"),
            ("category_id", "Категорія"),
            ("subcategory_id", "Підкатегорія"),
            ("product", "Продакт"),
        ]
        vars_map: dict[str, tk.StringVar] = {}
        for idx, (key, label) in enumerate(fields):
            ttk.Label(frame, text=label).grid(row=idx, column=0, sticky="w", pady=4)
            value = str(item[key]) if key in item.keys() else ""
            var = tk.StringVar(value=value)
            entry = ttk.Entry(frame, textvariable=var)
            if key == "sku":
                entry.configure(state="readonly")
            if key in {"supplier_id", "client_id", "category_id", "subcategory_id"}:
                entry.configure(state="readonly")
                lookup_map = {
                    "supplier_id": ("suppliers", "name"),
                    "client_id": ("clients", "name"),
                    "category_id": ("categories", "name"),
                    "subcategory_id": ("subcategories", "name"),
                }
                table, display = lookup_map[key]
                category_var = vars_map.get("category_id") if key == "subcategory_id" else None
                ttk.Button(
                    frame,
                    text="...",
                    command=lambda v=var, t=table, d=display, c=category_var: self.open_lookup(v, t, d, c),
                ).grid(row=idx, column=2, padx=4)
            if key == "uom_base":
                entry.destroy()
                entry = ttk.Combobox(frame, textvariable=var, values=["шт", "палета"], state="readonly")
            if key == "is_serial":
                entry.destroy()
                entry = ttk.Combobox(frame, textvariable=var, values=["Так", "Ні"], state="readonly")
                var.set("Так" if item["is_serial"] else "Ні")
            if key == "product":
                entry.configure(state="readonly")
            entry.grid(row=idx, column=1, sticky="ew", pady=4)
            vars_map[key] = var

        def save():
            self.db.execute(
                """
                UPDATE items SET client_id = ?, supplier_id = ?, brand = ?, uom_base = ?, volume = ?, weight = ?,
                    barcode = ?, is_serial = ?, category_id = ?, subcategory_id = ?, product = ?
                WHERE id = ?
                """,
                (
                    vars_map["client_id"].get(),
                    vars_map["supplier_id"].get(),
                    vars_map["brand"].get(),
                    vars_map["uom_base"].get(),
                    safe_float(vars_map["volume"].get()),
                    safe_float(vars_map["weight"].get()),
                    vars_map["barcode"].get(),
                    1 if vars_map["is_serial"].get() == "Так" else 0,
                    vars_map["category_id"].get(),
                    vars_map["subcategory_id"].get(),
                    vars_map["product"].get(),
                    item_id,
                ),
            )
            self.db.commit()
            self.dal.insert_audit(self.current_user["id"], "update", "items", item_id, "Оновлено")
            window.destroy()
            self.refresh_current()

        frame.columnconfigure(1, weight=1)
        ttk.Button(frame, text="Зберегти", command=save).grid(row=len(fields), column=0, columnspan=2, pady=8)
        window.grab_set()
        self.wait_window(window)

    def delete_item(self) -> None:
        if not self.ensure_perm("items.edit"):
            return
        item_id = self.get_selected_id()
        if not item_id:
            return
        if not messagebox.askyesno("Підтвердження", "Видалити запис?"):
            return
        self.db.execute("DELETE FROM items WHERE id = ?", (item_id,))
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "delete", "items", item_id, "Видалено")
        self.refresh_current()

    def create_client(self) -> None:
        if not self.ensure_perm("clients.edit"):
            return
        data = self.create_simple_form(
            "Створити клієнта",
            [
                ("code", "Код"),
                ("name", "Назва"),
                ("contract", "Договір"),
                ("sla", "SLA"),
                ("billing_type", "Тариф"),
            ],
        )
        if not data["code"].get():
            return
        self.db.execute(
            """
            INSERT INTO clients (id, name, code, contract, sla, billing_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                generate_id(),
                data["name"].get(),
                data["code"].get(),
                data["contract"].get(),
                data["sla"].get(),
                data["billing_type"].get() or "операції",
                now_str(),
            ),
        )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "create", "clients", data["code"].get(), "Створено")
        self.refresh_current()

    def edit_client(self) -> None:
        if not self.ensure_perm("clients.edit"):
            return
        client_id = self.get_selected_id()
        if not client_id:
            return
        client = self.db.execute("SELECT * FROM clients WHERE id = ?", (client_id,)).fetchone()
        if not client:
            return
        data = self.create_simple_form(
            "Редагувати клієнта",
            [
                ("code", "Код"),
                ("name", "Назва"),
                ("contract", "Договір"),
                ("sla", "SLA"),
                ("billing_type", "Тариф"),
            ],
            dict(client),
        )
        self.db.execute(
            """
            UPDATE clients SET name = ?, code = ?, contract = ?, sla = ?, billing_type = ? WHERE id = ?
            """,
            (
                data["name"].get(),
                data["code"].get(),
                data["contract"].get(),
                data["sla"].get(),
                data["billing_type"].get(),
                client_id,
            ),
        )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "update", "clients", client_id, "Оновлено")
        self.refresh_current()

    def delete_client(self) -> None:
        if not self.ensure_perm("clients.edit"):
            return
        client_id = self.get_selected_id()
        if not client_id:
            return
        if not messagebox.askyesno("Підтвердження", "Видалити клієнта?"):
            return
        self.db.execute("DELETE FROM clients WHERE id = ?", (client_id,))
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "delete", "clients", client_id, "Видалено")
        self.refresh_current()

    def create_supplier(self) -> None:
        if not self.ensure_perm("suppliers.edit"):
            return
        data = self.create_simple_form(
            "Створити постачальника",
            [("code", "Код"), ("name", "Назва"), ("contact", "Контакт")],
        )
        if not data["code"].get():
            return
        self.db.execute(
            """
            INSERT INTO suppliers (id, name, code, contact, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (generate_id(), data["name"].get(), data["code"].get(), data["contact"].get(), now_str()),
        )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "create", "suppliers", data["code"].get(), "Створено")
        self.refresh_current()

    def edit_supplier(self) -> None:
        if not self.ensure_perm("suppliers.edit"):
            return
        supplier_id = self.get_selected_id()
        if not supplier_id:
            return
        supplier = self.db.execute("SELECT * FROM suppliers WHERE id = ?", (supplier_id,)).fetchone()
        if not supplier:
            return
        data = self.create_simple_form(
            "Редагувати постачальника",
            [("code", "Код"), ("name", "Назва"), ("contact", "Контакт")],
            dict(supplier),
        )
        self.db.execute(
            "UPDATE suppliers SET name = ?, code = ?, contact = ? WHERE id = ?",
            (data["name"].get(), data["code"].get(), data["contact"].get(), supplier_id),
        )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "update", "suppliers", supplier_id, "Оновлено")
        self.refresh_current()

    def delete_supplier(self) -> None:
        if not self.ensure_perm("suppliers.edit"):
            return
        supplier_id = self.get_selected_id()
        if not supplier_id:
            return
        if not messagebox.askyesno("Підтвердження", "Видалити постачальника?"):
            return
        self.db.execute("DELETE FROM suppliers WHERE id = ?", (supplier_id,))
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "delete", "suppliers", supplier_id, "Видалено")
        self.refresh_current()

    def create_warehouse(self) -> None:
        if not self.ensure_perm("warehouses.edit"):
            return
        data = self.create_simple_form(
            "Створити склад",
            [("code", "Код"), ("name", "Назва"), ("address", "Адреса")],
        )
        if not data["code"].get():
            return
        self.db.execute(
            """
            INSERT INTO warehouses (id, name, code, address, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (generate_id(), data["name"].get(), data["code"].get(), data["address"].get(), now_str()),
        )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "create", "warehouses", data["code"].get(), "Створено")
        self.refresh_current()

    def edit_warehouse(self) -> None:
        if not self.ensure_perm("warehouses.edit"):
            return
        warehouse_id = self.get_selected_id()
        if not warehouse_id:
            return
        warehouse = self.db.execute("SELECT * FROM warehouses WHERE id = ?", (warehouse_id,)).fetchone()
        if not warehouse:
            return
        data = self.create_simple_form(
            "Редагувати склад",
            [("code", "Код"), ("name", "Назва"), ("address", "Адреса")],
            dict(warehouse),
        )
        self.db.execute(
            "UPDATE warehouses SET name = ?, code = ?, address = ? WHERE id = ?",
            (data["name"].get(), data["code"].get(), data["address"].get(), warehouse_id),
        )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "update", "warehouses", warehouse_id, "Оновлено")
        self.refresh_current()

    def delete_warehouse(self) -> None:
        if not self.ensure_perm("warehouses.edit"):
            return
        warehouse_id = self.get_selected_id()
        if not warehouse_id:
            return
        if not messagebox.askyesno("Підтвердження", "Видалити склад?"):
            return
        self.db.execute("DELETE FROM warehouses WHERE id = ?", (warehouse_id,))
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "delete", "warehouses", warehouse_id, "Видалено")
        self.refresh_current()

    def create_zone(self) -> None:
        if not self.ensure_perm("zones.edit"):
            return
        data = self.create_simple_form(
            "Створити зону",
            [("warehouse_id", "ID складу"), ("code", "Код"), ("name", "Назва"), ("zone_type", "Тип")],
        )
        if not data["code"].get():
            return
        self.db.execute(
            """
            INSERT INTO zones (id, warehouse_id, name, code, zone_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                generate_id(),
                data["warehouse_id"].get(),
                data["name"].get(),
                data["code"].get(),
                data["zone_type"].get(),
                now_str(),
            ),
        )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "create", "zones", data["code"].get(), "Створено")
        self.refresh_current()

    def edit_zone(self) -> None:
        if not self.ensure_perm("zones.edit"):
            return
        zone_id = self.get_selected_id()
        if not zone_id:
            return
        zone = self.db.execute("SELECT * FROM zones WHERE id = ?", (zone_id,)).fetchone()
        if not zone:
            return
        data = self.create_simple_form(
            "Редагувати зону",
            [("warehouse_id", "ID складу"), ("code", "Код"), ("name", "Назва"), ("zone_type", "Тип")],
            dict(zone),
        )
        self.db.execute(
            "UPDATE zones SET warehouse_id = ?, name = ?, code = ?, zone_type = ? WHERE id = ?",
            (
                data["warehouse_id"].get(),
                data["name"].get(),
                data["code"].get(),
                data["zone_type"].get(),
                zone_id,
            ),
        )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "update", "zones", zone_id, "Оновлено")
        self.refresh_current()

    def delete_zone(self) -> None:
        if not self.ensure_perm("zones.edit"):
            return
        zone_id = self.get_selected_id()
        if not zone_id:
            return
        if not messagebox.askyesno("Підтвердження", "Видалити зону?"):
            return
        self.db.execute("DELETE FROM zones WHERE id = ?", (zone_id,))
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "delete", "zones", zone_id, "Видалено")
        self.refresh_current()

    def create_location(self) -> None:
        if not self.ensure_perm("locations.edit"):
            return
        data = self.create_simple_form(
            "Створити комірку",
            [
                ("zone_id", "ID зони"),
                ("code", "Код"),
                ("location_type", "Тип"),
                ("capacity_volume", "Об'єм"),
                ("capacity_weight", "Вага"),
                ("capacity_pallets", "Палети"),
            ],
        )
        if not data["code"].get():
            return
        self.db.execute(
            """
            INSERT INTO locations (id, zone_id, code, location_type, capacity_volume, capacity_weight,
                                   capacity_pallets, allowed_category, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                generate_id(),
                data["zone_id"].get(),
                data["code"].get(),
                data["location_type"].get(),
                safe_float(data["capacity_volume"].get()),
                safe_float(data["capacity_weight"].get()),
                safe_int(data["capacity_pallets"].get()),
                None,
                now_str(),
            ),
        )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "create", "locations", data["code"].get(), "Створено")
        self.refresh_current()

    def edit_location(self) -> None:
        if not self.ensure_perm("locations.edit"):
            return
        location_id = self.get_selected_id()
        if not location_id:
            return
        loc = self.db.execute("SELECT * FROM locations WHERE id = ?", (location_id,)).fetchone()
        if not loc:
            return
        data = self.create_simple_form(
            "Редагувати комірку",
            [
                ("zone_id", "ID зони"),
                ("code", "Код"),
                ("location_type", "Тип"),
                ("capacity_volume", "Об'єм"),
                ("capacity_weight", "Вага"),
                ("capacity_pallets", "Палети"),
            ],
            dict(loc),
        )
        self.db.execute(
            """
            UPDATE locations SET zone_id = ?, code = ?, location_type = ?, capacity_volume = ?,
                capacity_weight = ?, capacity_pallets = ?
            WHERE id = ?
            """,
            (
                data["zone_id"].get(),
                data["code"].get(),
                data["location_type"].get(),
                safe_float(data["capacity_volume"].get()),
                safe_float(data["capacity_weight"].get()),
                safe_int(data["capacity_pallets"].get()),
                location_id,
            ),
        )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "update", "locations", location_id, "Оновлено")
        self.refresh_current()

    def delete_location(self) -> None:
        if not self.ensure_perm("locations.edit"):
            return
        location_id = self.get_selected_id()
        if not location_id:
            return
        if not messagebox.askyesno("Підтвердження", "Видалити комірку?"):
            return
        self.db.execute("DELETE FROM locations WHERE id = ?", (location_id,))
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "delete", "locations", location_id, "Видалено")
        self.refresh_current()

    def create_user(self) -> None:
        if not self.ensure_perm("users.edit"):
            return
        data = self.create_simple_form(
            "Створити користувача",
            [("username", "Логін"), ("full_name", "ПІБ"), ("role_id", "ID ролі"), ("password", "Пароль")],
        )
        if not data["username"].get():
            return
        salt, pw_hash = hash_password(data["password"].get() or "changeme")
        self.db.execute(
            """
            INSERT INTO users (id, username, full_name, role_id, salt, password_hash, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                generate_id(),
                data["username"].get(),
                data["full_name"].get(),
                data["role_id"].get(),
                salt,
                pw_hash,
                now_str(),
            ),
        )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "create", "users", data["username"].get(), "Створено")
        self.refresh_current()

    def edit_user(self) -> None:
        if not self.ensure_perm("users.edit"):
            return
        user_id = self.get_selected_id()
        if not user_id:
            return
        user = self.db.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user:
            return
        data = self.create_simple_form(
            "Редагувати користувача",
            [("username", "Логін"), ("full_name", "ПІБ"), ("role_id", "ID ролі"), ("password", "Новий пароль")],
            dict(user),
        )
        if data["password"].get():
            salt, pw_hash = hash_password(data["password"].get())
        else:
            salt, pw_hash = user["salt"], user["password_hash"]
        self.db.execute(
            """
            UPDATE users SET username = ?, full_name = ?, role_id = ?, salt = ?, password_hash = ?
            WHERE id = ?
            """,
            (
                data["username"].get(),
                data["full_name"].get(),
                data["role_id"].get(),
                salt,
                pw_hash,
                user_id,
            ),
        )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "update", "users", user_id, "Оновлено")
        self.refresh_current()

    def delete_user(self) -> None:
        if not self.ensure_perm("users.edit"):
            return
        user_id = self.get_selected_id()
        if not user_id:
            return
        if not messagebox.askyesno("Підтвердження", "Видалити користувача?"):
            return
        self.db.execute("DELETE FROM users WHERE id = ?", (user_id,))
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "delete", "users", user_id, "Видалено")
        self.refresh_current()

    def create_role(self) -> None:
        if not self.ensure_perm("roles.edit"):
            return
        data = self.create_simple_form("Створити роль", [("name", "Назва"), ("permissions", "Права через кому")])
        if not data["name"].get():
            return
        role_id = generate_id()
        self.db.execute("INSERT INTO roles (id, name) VALUES (?, ?)", (role_id, data["name"].get()))
        perms = [perm.strip() for perm in data["permissions"].get().split(",") if perm.strip()]
        for perm in perms:
            self.db.execute(
                "INSERT INTO role_permissions (id, role_id, permission) VALUES (?, ?, ?)",
                (generate_id(), role_id, perm),
            )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "create", "roles", role_id, "Створено")
        self.rbac.load()
        self.refresh_current()

    def edit_role(self) -> None:
        if not self.ensure_perm("roles.edit"):
            return
        role_id = self.get_selected_id()
        if not role_id:
            return
        role = self.db.execute("SELECT * FROM roles WHERE id = ?", (role_id,)).fetchone()
        if not role:
            return
        current_perms = ", ".join(self.dal.get_permissions_for_role(role_id))
        data = self.create_simple_form(
            "Редагувати роль",
            [("name", "Назва"), ("permissions", "Права через кому")],
            {"name": role["name"], "permissions": current_perms},
        )
        self.db.execute("UPDATE roles SET name = ? WHERE id = ?", (data["name"].get(), role_id))
        self.db.execute("DELETE FROM role_permissions WHERE role_id = ?", (role_id,))
        perms = [perm.strip() for perm in data["permissions"].get().split(",") if perm.strip()]
        for perm in perms:
            self.db.execute(
                "INSERT INTO role_permissions (id, role_id, permission) VALUES (?, ?, ?)",
                (generate_id(), role_id, perm),
            )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "update", "roles", role_id, "Оновлено")
        self.rbac.load()
        self.refresh_current()

    def delete_role(self) -> None:
        if not self.ensure_perm("roles.edit"):
            return
        role_id = self.get_selected_id()
        if not role_id:
            return
        if not messagebox.askyesno("Підтвердження", "Видалити роль?"):
            return
        self.db.execute("DELETE FROM roles WHERE id = ?", (role_id,))
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "delete", "roles", role_id, "Видалено")
        self.rbac.load()
        self.refresh_current()

    def create_inbound(self) -> None:
        if not self.ensure_perm("inbound.edit"):
            return
        data = self.create_simple_form(
            "Створити замовлення на приймання",
            [
                ("number", "Номер"),
                ("client_id", "ID клієнта"),
                ("supplier_id", "ID постачальника"),
                ("warehouse_id", "ID складу"),
                ("expected_date", "Очікувана дата"),
            ],
        )
        if not data["number"].get():
            return
        order_id = generate_id()
        self.db.execute(
            """
            INSERT INTO inbound_orders (id, number, status, client_id, supplier_id, warehouse_id,
                                   expected_date, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                data["number"].get(),
                "Чернетка",
                data["client_id"].get(),
                data["supplier_id"].get(),
                data["warehouse_id"].get(),
                data["expected_date"].get(),
                self.current_user["username"],
                now_str(),
            ),
        )
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "create", "inbound_orders", order_id, "Створено")
        self.refresh_current()
        self.manage_inbound_lines(order_id)

    def edit_inbound(self) -> None:
        if not self.ensure_perm("inbound.edit"):
            return
        order_id = self.get_selected_id()
        if not order_id:
            return
        order = self.db.execute("SELECT * FROM inbound_orders WHERE id = ?", (order_id,)).fetchone()
        if not order:
            return
        if order["status"] == "Проведено":
            messagebox.showerror("Помилка", "Проведений документ не редагується")
            return
        data = self.create_simple_form(
            "Редагувати замовлення",
            [
                ("number", "Номер"),
                ("client_id", "ID клієнта"),
                ("supplier_id", "ID постачальника"),
                ("warehouse_id", "ID складу"),
                ("expected_date", "Очікувана дата"),
            ],
            dict(order),
        )
        self.db.execute(
            """
            UPDATE inbound_orders SET number = ?, client_id = ?, supplier_id = ?, warehouse_id = ?, expected_date = ?
            WHERE id = ?
            """,
            (
                data["number"].get(),
                data["client_id"].get(),
                data["supplier_id"].get(),
                data["warehouse_id"].get(),
                data["expected_date"].get(),
                order_id,
            ),
        )
        self.db.commit()
        self.manage_inbound_lines(order_id)
        self.dal.insert_audit(self.current_user["id"], "update", "inbound_orders", order_id, "Оновлено")
        self.refresh_current()

    def delete_inbound(self) -> None:
        if not self.ensure_perm("inbound.edit"):
            return
        order_id = self.get_selected_id()
        if not order_id:
            return
        order = self.db.execute("SELECT status FROM inbound_orders WHERE id = ?", (order_id,)).fetchone()
        if order and order["status"] == "Проведено":
            messagebox.showerror("Помилка", "Проведений документ не видаляється")
            return
        if not messagebox.askyesno("Підтвердження", "Видалити документ?"):
            return
        self.db.execute("DELETE FROM inbound_orders WHERE id = ?", (order_id,))
        self.db.commit()
        self.dal.insert_audit(self.current_user["id"], "delete", "inbound_orders", order_id, "Видалено")
        self.refresh_current()

    def manage_inbound_lines(self, order_id: str) -> None:
        window = tk.Toplevel(self)
        window.title("Позиції приймання")
        window.geometry("700x400")
        frame = ttk.Frame(window, padding=10)
        frame.pack(fill="both", expand=True)
        table = TableFrame(frame, [("id", "ID"), ("item_id", "Товар"), ("qty", "К-сть")])
        table.pack(fill="both", expand=True)

        def load():
            rows = [
                (row["id"], row["item_id"], row["qty_plan"])
                for row in self.db.execute("SELECT * FROM inbound_order_lines WHERE order_id = ?", (order_id,)).fetchall()
            ]
            table.set_rows(rows)

        def add_line():
            data = self.create_simple_form("Додати позицію", [("item_id", "ID товару"), ("qty_plan", "К-сть")])
            if not data["item_id"].get():
                return
            self.db.execute(
                "INSERT INTO inbound_order_lines (id, order_id, item_id, qty_plan) VALUES (?, ?, ?, ?)",
                (generate_id(), order_id, data["item_id"].get(), safe_float(data["qty_plan"].get())),
            )
            self.db.commit()
            load()

        def delete_line():
            selected = table.selected_values()
            if not selected:
                return
            self.db.execute("DELETE FROM inbound_order_lines WHERE id = ?", (selected[0],))
            self.db.commit()
            load()

        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill="x", pady=4)
        ttk.Button(btn_frame, text="Додати", command=add_line).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Видалити", command=delete_line).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Закрити", command=window.destroy).pack(side="right")
        load()
        window.grab_set()
        self.wait_window(window)

    def post_inbound(self) -> None:
        if not self.ensure_perm("inbound.post"):
            return
        order_id = self.get_selected_id()
        if not order_id:
            return
        order = self.db.execute("SELECT * FROM inbound_orders WHERE id = ?", (order_id,)).fetchone()
        if not order or order["status"] == "Проведено":
            return
        lines = self.db.execute("SELECT * FROM inbound_order_lines WHERE order_id = ?", (order_id,)).fetchall()
        if not lines:
            messagebox.showerror("Помилка", "Немає позицій")
            return
        try:
            for line in lines:
                self.service.create_stock_move(
                    "IN_RECEIPT",
                    "INBOUND",
                    order_id,
                    order["client_id"],
                    order["warehouse_id"],
                    line["item_id"],
                    line["qty_plan"],
                    self.current_user["username"],
                    location_to=None,
                )
            self.db.execute("UPDATE inbound_orders SET status = ? WHERE id = ?", ("Проведено", order_id))
            self.db.commit()
            self.dal.insert_audit(self.current_user["id"], "post", "inbound_orders", order_id, "Проведено")
            self.refresh_current()
        except Exception as exc:
            self.db.rollback()
            messagebox.showerror("Помилка", str(exc))

    def create_receipt(self) -> None:
        if not self.ensure_perm("receipts.edit"):
            return
        data = self.create_simple_form(
            "Створити приймання",
            [
                ("number", "Номер"),
                ("order_id", "ID замовлення"),
                ("client_id", "ID клієнта"),
                ("warehouse_id", "ID складу"),
            ],
        )
        if not data["number"].get():
            return
        receipt_id = generate_id()
        self.db.execute(
            """
            INSERT INTO receipts (id, number, status, order_id, client_id, warehouse_id, received_at, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                receipt_id,
                data["number"].get(),
                "Чернетка",
                data["order_id"].get(),
                data["client_id"].get(),
                data["warehouse_id"].get(),
                now_str(),
                self.current_user["username"],
                now_str(),
            ),
        )
        self.db.commit()
        self.manage_receipt_lines(receipt_id)
        self.refresh_current()

    def edit_receipt(self) -> None:
        if not self.ensure_perm("receipts.edit"):
            return
        receipt_id = self.get_selected_id()
        if not receipt_id:
            return
        receipt = self.db.execute("SELECT * FROM receipts WHERE id = ?", (receipt_id,)).fetchone()
        if receipt["status"] == "Проведено":
            messagebox.showerror("Помилка", "Проведений документ не редагується")
            return
        data = self.create_simple_form(
            "Редагувати приймання",
            [
                ("number", "Номер"),
                ("order_id", "ID замовлення"),
                ("client_id", "ID клієнта"),
                ("warehouse_id", "ID складу"),
            ],
            dict(receipt),
        )
        self.db.execute(
            """
            UPDATE receipts SET number = ?, order_id = ?, client_id = ?, warehouse_id = ? WHERE id = ?
            """,
            (
                data["number"].get(),
                data["order_id"].get(),
                data["client_id"].get(),
                data["warehouse_id"].get(),
                receipt_id,
            ),
        )
        self.db.commit()
        self.manage_receipt_lines(receipt_id)
        self.refresh_current()

    def delete_receipt(self) -> None:
        if not self.ensure_perm("receipts.edit"):
            return
        receipt_id = self.get_selected_id()
        if not receipt_id:
            return
        receipt = self.db.execute("SELECT status FROM receipts WHERE id = ?", (receipt_id,)).fetchone()
        if receipt and receipt["status"] == "Проведено":
            messagebox.showerror("Помилка", "Проведений документ не видаляється")
            return
        if not messagebox.askyesno("Підтвердження", "Видалити приймання?"):
            return
        self.db.execute("DELETE FROM receipts WHERE id = ?", (receipt_id,))
        self.db.commit()
        self.refresh_current()

    def manage_receipt_lines(self, receipt_id: str) -> None:
        window = tk.Toplevel(self)
        window.title("Позиції приймання")
        window.geometry("700x400")
        frame = ttk.Frame(window, padding=10)
        frame.pack(fill="both", expand=True)
        table = TableFrame(frame, [("id", "ID"), ("item_id", "Товар"), ("qty", "К-сть")])
        table.pack(fill="both", expand=True)

        def load():
            rows = [
                (row["id"], row["item_id"], row["qty"])
                for row in self.db.execute("SELECT * FROM receipt_lines WHERE receipt_id = ?", (receipt_id,)).fetchall()
            ]
            table.set_rows(rows)

        def add_line():
            data = self.create_simple_form("Додати позицію", [("item_id", "ID товару"), ("qty", "К-сть")])
            if not data["item_id"].get():
                return
            self.db.execute(
                "INSERT INTO receipt_lines (id, receipt_id, item_id, qty) VALUES (?, ?, ?, ?)",
                (generate_id(), receipt_id, data["item_id"].get(), safe_float(data["qty"].get())),
            )
            self.db.commit()
            load()

        def delete_line():
            selected = table.selected_values()
            if not selected:
                return
            self.db.execute("DELETE FROM receipt_lines WHERE id = ?", (selected[0],))
            self.db.commit()
            load()

        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill="x", pady=4)
        ttk.Button(btn_frame, text="Додати", command=add_line).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Видалити", command=delete_line).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Закрити", command=window.destroy).pack(side="right")
        load()
        window.grab_set()
        self.wait_window(window)

    def post_receipt(self) -> None:
        if not self.ensure_perm("receipts.post"):
            return
        receipt_id = self.get_selected_id()
        if not receipt_id:
            return
        receipt = self.db.execute("SELECT * FROM receipts WHERE id = ?", (receipt_id,)).fetchone()
        if receipt["status"] == "Проведено":
            return
        lines = self.db.execute("SELECT * FROM receipt_lines WHERE receipt_id = ?", (receipt_id,)).fetchall()
        if not lines:
            messagebox.showerror("Помилка", "Немає позицій")
            return
        try:
            for line in lines:
                self.service.create_stock_move(
                    "IN_RECEIPT",
                    "RECEIPT",
                    receipt_id,
                    receipt["client_id"],
                    receipt["warehouse_id"],
                    line["item_id"],
                    line["qty"],
                    self.current_user["username"],
                    location_to=None,
                )
            self.db.execute("UPDATE receipts SET status = ? WHERE id = ?", ("Проведено", receipt_id))
            self.db.commit()
            self.refresh_current()
        except Exception as exc:
            self.db.rollback()
            messagebox.showerror("Помилка", str(exc))

    def create_putaway(self) -> None:
        if not self.ensure_perm("putaway.edit"):
            return
        data = self.create_simple_form(
            "Створити putaway",
            [("receipt_id", "ID приймання"), ("item_id", "ID товару"), ("qty", "К-сть"), ("location_to", "Комірка")],
        )
        if not data["receipt_id"].get():
            return
        self.db.execute(
            """
            INSERT INTO putaway_tasks (id, receipt_id, item_id, qty, location_to, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                generate_id(),
                data["receipt_id"].get(),
                data["item_id"].get(),
                safe_float(data["qty"].get()),
                data["location_to"].get(),
                "Чернетка",
                now_str(),
            ),
        )
        self.db.commit()
        self.refresh_current()

    def edit_putaway(self) -> None:
        if not self.ensure_perm("putaway.edit"):
            return
        task_id = self.get_selected_id()
        if not task_id:
            return
        task = self.db.execute("SELECT * FROM putaway_tasks WHERE id = ?", (task_id,)).fetchone()
        data = self.create_simple_form(
            "Редагувати putaway",
            [("receipt_id", "ID приймання"), ("item_id", "ID товару"), ("qty", "К-сть"), ("location_to", "Комірка"), ("status", "Статус")],
            dict(task),
        )
        self.db.execute(
            """
            UPDATE putaway_tasks SET receipt_id = ?, item_id = ?, qty = ?, location_to = ?, status = ?
            WHERE id = ?
            """,
            (
                data["receipt_id"].get(),
                data["item_id"].get(),
                safe_float(data["qty"].get()),
                data["location_to"].get(),
                data["status"].get(),
                task_id,
            ),
        )
        self.db.commit()
        self.refresh_current()

    def delete_putaway(self) -> None:
        if not self.ensure_perm("putaway.edit"):
            return
        task_id = self.get_selected_id()
        if not task_id:
            return
        if not messagebox.askyesno("Підтвердження", "Видалити putaway?"):
            return
        self.db.execute("DELETE FROM putaway_tasks WHERE id = ?", (task_id,))
        self.db.commit()
        self.refresh_current()

    def create_outbound(self) -> None:
        if not self.ensure_perm("outbound.edit"):
            return
        data = self.create_simple_form(
            "Створити замовлення клієнта",
            [
                ("number", "Номер"),
                ("client_id", "ID клієнта"),
                ("warehouse_id", "ID складу"),
                ("ship_to", "Адреса доставки"),
                ("carrier", "Перевізник"),
                ("priority", "Пріоритет"),
                ("deadline", "Дедлайн"),
            ],
        )
        if not data["number"].get():
            return
        order_id = generate_id()
        self.db.execute(
            """
            INSERT INTO outbound_orders (id, number, status, client_id, warehouse_id, ship_to, carrier,
                                    priority, deadline, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                data["number"].get(),
                "Чернетка",
                data["client_id"].get(),
                data["warehouse_id"].get(),
                data["ship_to"].get(),
                data["carrier"].get(),
                data["priority"].get(),
                data["deadline"].get(),
                self.current_user["username"],
                now_str(),
            ),
        )
        self.db.commit()
        self.manage_outbound_lines(order_id)
        self.refresh_current()

    def edit_outbound(self) -> None:
        if not self.ensure_perm("outbound.edit"):
            return
        order_id = self.get_selected_id()
        if not order_id:
            return
        order = self.db.execute("SELECT * FROM outbound_orders WHERE id = ?", (order_id,)).fetchone()
        if order["status"] == "Проведено":
            messagebox.showerror("Помилка", "Проведений документ не редагується")
            return
        data = self.create_simple_form(
            "Редагувати замовлення",
            [
                ("number", "Номер"),
                ("client_id", "ID клієнта"),
                ("warehouse_id", "ID складу"),
                ("ship_to", "Адреса доставки"),
                ("carrier", "Перевізник"),
                ("priority", "Пріоритет"),
                ("deadline", "Дедлайн"),
            ],
            dict(order),
        )
        self.db.execute(
            """
            UPDATE outbound_orders SET number = ?, client_id = ?, warehouse_id = ?, ship_to = ?, carrier = ?,
                priority = ?, deadline = ?
            WHERE id = ?
            """,
            (
                data["number"].get(),
                data["client_id"].get(),
                data["warehouse_id"].get(),
                data["ship_to"].get(),
                data["carrier"].get(),
                data["priority"].get(),
                data["deadline"].get(),
                order_id,
            ),
        )
        self.db.commit()
        self.manage_outbound_lines(order_id)
        self.refresh_current()

    def delete_outbound(self) -> None:
        if not self.ensure_perm("outbound.edit"):
            return
        order_id = self.get_selected_id()
        if not order_id:
            return
        order = self.db.execute("SELECT status FROM outbound_orders WHERE id = ?", (order_id,)).fetchone()
        if order and order["status"] == "Проведено":
            messagebox.showerror("Помилка", "Проведений документ не видаляється")
            return
        if not messagebox.askyesno("Підтвердження", "Видалити документ?"):
            return
        self.db.execute("DELETE FROM outbound_orders WHERE id = ?", (order_id,))
        self.db.commit()
        self.refresh_current()

    def manage_outbound_lines(self, order_id: str) -> None:
        window = tk.Toplevel(self)
        window.title("Позиції відвантаження")
        window.geometry("700x400")
        frame = ttk.Frame(window, padding=10)
        frame.pack(fill="both", expand=True)
        table = TableFrame(frame, [("id", "ID"), ("item_id", "Товар"), ("qty", "К-сть")])
        table.pack(fill="both", expand=True)

        def load():
            rows = [
                (row["id"], row["item_id"], row["qty_plan"])
                for row in self.db.execute("SELECT * FROM outbound_order_lines WHERE order_id = ?", (order_id,)).fetchall()
            ]
            table.set_rows(rows)

        def add_line():
            data = self.create_simple_form("Додати позицію", [("item_id", "ID товару"), ("qty_plan", "К-сть")])
            if not data["item_id"].get():
                return
            self.db.execute(
                "INSERT INTO outbound_order_lines (id, order_id, item_id, qty_plan) VALUES (?, ?, ?, ?)",
                (generate_id(), order_id, data["item_id"].get(), safe_float(data["qty_plan"].get())),
            )
            self.db.commit()
            load()

        def delete_line():
            selected = table.selected_values()
            if not selected:
                return
            self.db.execute("DELETE FROM outbound_order_lines WHERE id = ?", (selected[0],))
            self.db.commit()
            load()

        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill="x", pady=4)
        ttk.Button(btn_frame, text="Додати", command=add_line).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Видалити", command=delete_line).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Закрити", command=window.destroy).pack(side="right")
        load()
        window.grab_set()
        self.wait_window(window)

    def post_outbound(self) -> None:
        if not self.ensure_perm("outbound.post"):
            return
        order_id = self.get_selected_id()
        if not order_id:
            return
        order = self.db.execute("SELECT * FROM outbound_orders WHERE id = ?", (order_id,)).fetchone()
        if order["status"] == "Проведено":
            return
        lines = self.db.execute("SELECT * FROM outbound_order_lines WHERE order_id = ?", (order_id,)).fetchall()
        if not lines:
            messagebox.showerror("Помилка", "Немає позицій")
            return
        try:
            for line in lines:
                self.service.create_stock_move(
                    "SHIP",
                    "OUTBOUND",
                    order_id,
                    order["client_id"],
                    order["warehouse_id"],
                    line["item_id"],
                    -float(line["qty_plan"]),
                    self.current_user["username"],
                    location_from=None,
                )
            self.db.execute("UPDATE outbound_orders SET status = ? WHERE id = ?", ("Проведено", order_id))
            self.db.commit()
            self.refresh_current()
        except Exception as exc:
            self.db.rollback()
            messagebox.showerror("Помилка", str(exc))

    def create_inventory_count(self) -> None:
        if not self.ensure_perm("inventory.edit"):
            return
        data = self.create_simple_form(
            "Створити інвентаризацію",
            [("number", "Номер"), ("warehouse_id", "ID складу"), ("zone_id", "ID зони"), ("location_id", "ID комірки")],
        )
        if not data["number"].get():
            return
        count_id = generate_id()
        self.db.execute(
            """
            INSERT INTO inventory_counts (id, number, status, warehouse_id, zone_id, location_id, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                count_id,
                data["number"].get(),
                "Чернетка",
                data["warehouse_id"].get(),
                data["zone_id"].get(),
                data["location_id"].get(),
                self.current_user["username"],
                now_str(),
            ),
        )
        self.db.commit()
        self.manage_inventory_lines(count_id)
        self.refresh_current()

    def edit_inventory_count(self) -> None:
        if not self.ensure_perm("inventory.edit"):
            return
        count_id = self.get_selected_id()
        if not count_id:
            return
        count = self.db.execute("SELECT * FROM inventory_counts WHERE id = ?", (count_id,)).fetchone()
        if count["status"] == "Проведено":
            messagebox.showerror("Помилка", "Проведений документ не редагується")
            return
        data = self.create_simple_form(
            "Редагувати інвентаризацію",
            [("number", "Номер"), ("warehouse_id", "ID складу"), ("zone_id", "ID зони"), ("location_id", "ID комірки")],
            dict(count),
        )
        self.db.execute(
            """
            UPDATE inventory_counts SET number = ?, warehouse_id = ?, zone_id = ?, location_id = ? WHERE id = ?
            """,
            (
                data["number"].get(),
                data["warehouse_id"].get(),
                data["zone_id"].get(),
                data["location_id"].get(),
                count_id,
            ),
        )
        self.db.commit()
        self.manage_inventory_lines(count_id)
        self.refresh_current()

    def delete_inventory_count(self) -> None:
        if not self.ensure_perm("inventory.edit"):
            return
        count_id = self.get_selected_id()
        if not count_id:
            return
        count = self.db.execute("SELECT status FROM inventory_counts WHERE id = ?", (count_id,)).fetchone()
        if count and count["status"] == "Проведено":
            messagebox.showerror("Помилка", "Проведений документ не видаляється")
            return
        if not messagebox.askyesno("Підтвердження", "Видалити інвентаризацію?"):
            return
        self.db.execute("DELETE FROM inventory_counts WHERE id = ?", (count_id,))
        self.db.commit()
        self.refresh_current()

    def manage_inventory_lines(self, count_id: str) -> None:
        window = tk.Toplevel(self)
        window.title("Позиції інвентаризації")
        window.geometry("700x400")
        frame = ttk.Frame(window, padding=10)
        frame.pack(fill="both", expand=True)
        table = TableFrame(frame, [("id", "ID"), ("item_id", "Товар"), ("qty_system", "Системна"), ("qty_count", "Факт")])
        table.pack(fill="both", expand=True)

        def load():
            rows = [
                (row["id"], row["item_id"], row["qty_system"], row["qty_count"])
                for row in self.db.execute("SELECT * FROM inventory_count_lines WHERE count_id = ?", (count_id,)).fetchall()
            ]
            table.set_rows(rows)

        def add_line():
            data = self.create_simple_form(
                "Додати позицію",
                [("item_id", "ID товару"), ("qty_system", "Системна"), ("qty_count", "Факт")],
            )
            if not data["item_id"].get():
                return
            self.db.execute(
                """
                INSERT INTO inventory_count_lines (id, count_id, item_id, qty_system, qty_count)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    generate_id(),
                    count_id,
                    data["item_id"].get(),
                    safe_float(data["qty_system"].get()),
                    safe_float(data["qty_count"].get()),
                ),
            )
            self.db.commit()
            load()

        def delete_line():
            selected = table.selected_values()
            if not selected:
                return
            self.db.execute("DELETE FROM inventory_count_lines WHERE id = ?", (selected[0],))
            self.db.commit()
            load()

        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill="x", pady=4)
        ttk.Button(btn_frame, text="Додати", command=add_line).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Видалити", command=delete_line).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Закрити", command=window.destroy).pack(side="right")
        load()
        window.grab_set()
        self.wait_window(window)

    def post_inventory_count(self) -> None:
        if not self.ensure_perm("inventory.post"):
            return
        count_id = self.get_selected_id()
        if not count_id:
            return
        count = self.db.execute("SELECT * FROM inventory_counts WHERE id = ?", (count_id,)).fetchone()
        if count["status"] == "Проведено":
            return
        lines = self.db.execute("SELECT * FROM inventory_count_lines WHERE count_id = ?", (count_id,)).fetchall()
        if not lines:
            messagebox.showerror("Помилка", "Немає позицій")
            return
        try:
            for line in lines:
                delta = line["qty_count"] - line["qty_system"]
                if delta == 0:
                    continue
                client_id = self.get_item_client(line["item_id"])
                if not client_id:
                    raise ValueError("Невірний клієнт для товару")
                self.service.create_stock_move(
                    "ADJUSTMENT",
                    "INVENTORY",
                    count_id,
                    client_id,
                    count["warehouse_id"],
                    line["item_id"],
                    delta,
                    self.current_user["username"],
                )
            self.db.execute("UPDATE inventory_counts SET status = ? WHERE id = ?", ("Проведено", count_id))
            self.db.commit()
            self.refresh_current()
        except Exception as exc:
            self.db.rollback()
            messagebox.showerror("Помилка", str(exc))

    def create_return(self) -> None:
        if not self.ensure_perm("returns.edit"):
            return
        data = self.create_simple_form(
            "Створити повернення",
            [("number", "Номер"), ("client_id", "ID клієнта"), ("warehouse_id", "ID складу")],
        )
        if not data["number"].get():
            return
        return_id = generate_id()
        self.db.execute(
            """
            INSERT INTO returns (id, number, status, client_id, warehouse_id, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                return_id,
                data["number"].get(),
                "Чернетка",
                data["client_id"].get(),
                data["warehouse_id"].get(),
                self.current_user["username"],
                now_str(),
            ),
        )
        self.db.commit()
        self.manage_return_lines(return_id)
        self.refresh_current()

    def edit_return(self) -> None:
        if not self.ensure_perm("returns.edit"):
            return
        return_id = self.get_selected_id()
        if not return_id:
            return
        ret = self.db.execute("SELECT * FROM returns WHERE id = ?", (return_id,)).fetchone()
        if ret["status"] == "Проведено":
            messagebox.showerror("Помилка", "Проведений документ не редагується")
            return
        data = self.create_simple_form(
            "Редагувати повернення",
            [("number", "Номер"), ("client_id", "ID клієнта"), ("warehouse_id", "ID складу")],
            dict(ret),
        )
        self.db.execute(
            "UPDATE returns SET number = ?, client_id = ?, warehouse_id = ? WHERE id = ?",
            (data["number"].get(), data["client_id"].get(), data["warehouse_id"].get(), return_id),
        )
        self.db.commit()
        self.manage_return_lines(return_id)
        self.refresh_current()

    def delete_return(self) -> None:
        if not self.ensure_perm("returns.edit"):
            return
        return_id = self.get_selected_id()
        if not return_id:
            return
        ret = self.db.execute("SELECT status FROM returns WHERE id = ?", (return_id,)).fetchone()
        if ret and ret["status"] == "Проведено":
            messagebox.showerror("Помилка", "Проведений документ не видаляється")
            return
        if not messagebox.askyesno("Підтвердження", "Видалити повернення?"):
            return
        self.db.execute("DELETE FROM returns WHERE id = ?", (return_id,))
        self.db.commit()
        self.refresh_current()

    def manage_return_lines(self, return_id: str) -> None:
        window = tk.Toplevel(self)
        window.title("Позиції повернення")
        window.geometry("700x400")
        frame = ttk.Frame(window, padding=10)
        frame.pack(fill="both", expand=True)
        table = TableFrame(frame, [("id", "ID"), ("item_id", "Товар"), ("qty", "К-сть"), ("disp", "Стан")])
        table.pack(fill="both", expand=True)

        def load():
            rows = [
                (row["id"], row["item_id"], row["qty"], row["disposition"])
                for row in self.db.execute("SELECT * FROM return_lines WHERE return_id = ?", (return_id,)).fetchall()
            ]
            table.set_rows(rows)

        def add_line():
            data = self.create_simple_form(
                "Додати позицію",
                [("item_id", "ID товару"), ("qty", "К-сть"), ("disposition", "Стан")],
            )
            if not data["item_id"].get():
                return
            self.db.execute(
                """
                INSERT INTO return_lines (id, return_id, item_id, qty, disposition)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    generate_id(),
                    return_id,
                    data["item_id"].get(),
                    safe_float(data["qty"].get()),
                    data["disposition"].get() or "годне",
                ),
            )
            self.db.commit()
            load()

        def delete_line():
            selected = table.selected_values()
            if not selected:
                return
            self.db.execute("DELETE FROM return_lines WHERE id = ?", (selected[0],))
            self.db.commit()
            load()

        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill="x", pady=4)
        ttk.Button(btn_frame, text="Додати", command=add_line).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Видалити", command=delete_line).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Закрити", command=window.destroy).pack(side="right")
        load()
        window.grab_set()
        self.wait_window(window)

    def post_return(self) -> None:
        if not self.ensure_perm("returns.post"):
            return
        return_id = self.get_selected_id()
        if not return_id:
            return
        ret = self.db.execute("SELECT * FROM returns WHERE id = ?", (return_id,)).fetchone()
        if ret["status"] == "Проведено":
            return
        lines = self.db.execute("SELECT * FROM return_lines WHERE return_id = ?", (return_id,)).fetchall()
        if not lines:
            messagebox.showerror("Помилка", "Немає позицій")
            return
        try:
            for line in lines:
                self.service.create_stock_move(
                    "RETURN",
                    "RETURN",
                    return_id,
                    ret["client_id"],
                    ret["warehouse_id"],
                    line["item_id"],
                    line["qty"],
                    self.current_user["username"],
                )
            self.db.execute("UPDATE returns SET status = ? WHERE id = ?", ("Проведено", return_id))
            self.db.commit()
            self.refresh_current()
        except Exception as exc:
            self.db.rollback()
            messagebox.showerror("Помилка", str(exc))

    def create_writeoff(self) -> None:
        if not self.ensure_perm("writeoffs.edit"):
            return
        data = self.create_simple_form(
            "Створити списання",
            [("number", "Номер"), ("warehouse_id", "ID складу"), ("reason", "Причина")],
        )
        if not data["number"].get():
            return
        writeoff_id = generate_id()
        self.db.execute(
            """
            INSERT INTO writeoffs (id, number, status, warehouse_id, reason, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                writeoff_id,
                data["number"].get(),
                "Чернетка",
                data["warehouse_id"].get(),
                data["reason"].get(),
                self.current_user["username"],
                now_str(),
            ),
        )
        self.db.commit()
        self.manage_writeoff_lines(writeoff_id)
        self.refresh_current()

    def edit_writeoff(self) -> None:
        if not self.ensure_perm("writeoffs.edit"):
            return
        writeoff_id = self.get_selected_id()
        if not writeoff_id:
            return
        writeoff = self.db.execute("SELECT * FROM writeoffs WHERE id = ?", (writeoff_id,)).fetchone()
        if writeoff["status"] == "Проведено":
            messagebox.showerror("Помилка", "Проведений документ не редагується")
            return
        data = self.create_simple_form(
            "Редагувати списання",
            [("number", "Номер"), ("warehouse_id", "ID складу"), ("reason", "Причина")],
            dict(writeoff),
        )
        self.db.execute(
            "UPDATE writeoffs SET number = ?, warehouse_id = ?, reason = ? WHERE id = ?",
            (data["number"].get(), data["warehouse_id"].get(), data["reason"].get(), writeoff_id),
        )
        self.db.commit()
        self.manage_writeoff_lines(writeoff_id)
        self.refresh_current()

    def delete_writeoff(self) -> None:
        if not self.ensure_perm("writeoffs.edit"):
            return
        writeoff_id = self.get_selected_id()
        if not writeoff_id:
            return
        writeoff = self.db.execute("SELECT status FROM writeoffs WHERE id = ?", (writeoff_id,)).fetchone()
        if writeoff and writeoff["status"] == "Проведено":
            messagebox.showerror("Помилка", "Проведений документ не видаляється")
            return
        if not messagebox.askyesno("Підтвердження", "Видалити списання?"):
            return
        self.db.execute("DELETE FROM writeoffs WHERE id = ?", (writeoff_id,))
        self.db.commit()
        self.refresh_current()

    def manage_writeoff_lines(self, writeoff_id: str) -> None:
        window = tk.Toplevel(self)
        window.title("Позиції списання")
        window.geometry("700x400")
        frame = ttk.Frame(window, padding=10)
        frame.pack(fill="both", expand=True)
        table = TableFrame(frame, [("id", "ID"), ("item_id", "Товар"), ("qty", "К-сть")])
        table.pack(fill="both", expand=True)

        def load():
            rows = [
                (row["id"], row["item_id"], row["qty"])
                for row in self.db.execute("SELECT * FROM writeoff_lines WHERE writeoff_id = ?", (writeoff_id,)).fetchall()
            ]
            table.set_rows(rows)

        def add_line():
            data = self.create_simple_form("Додати позицію", [("item_id", "ID товару"), ("qty", "К-сть")])
            if not data["item_id"].get():
                return
            self.db.execute(
                "INSERT INTO writeoff_lines (id, writeoff_id, item_id, qty) VALUES (?, ?, ?, ?)",
                (generate_id(), writeoff_id, data["item_id"].get(), safe_float(data["qty"].get())),
            )
            self.db.commit()
            load()

        def delete_line():
            selected = table.selected_values()
            if not selected:
                return
            self.db.execute("DELETE FROM writeoff_lines WHERE id = ?", (selected[0],))
            self.db.commit()
            load()

        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill="x", pady=4)
        ttk.Button(btn_frame, text="Додати", command=add_line).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Видалити", command=delete_line).pack(side="left", padx=4)
        ttk.Button(btn_frame, text="Закрити", command=window.destroy).pack(side="right")
        load()
        window.grab_set()
        self.wait_window(window)

    def post_writeoff(self) -> None:
        if not self.ensure_perm("writeoffs.post"):
            return
        writeoff_id = self.get_selected_id()
        if not writeoff_id:
            return
        writeoff = self.db.execute("SELECT * FROM writeoffs WHERE id = ?", (writeoff_id,)).fetchone()
        if writeoff["status"] == "Проведено":
            return
        lines = self.db.execute("SELECT * FROM writeoff_lines WHERE writeoff_id = ?", (writeoff_id,)).fetchall()
        if not lines:
            messagebox.showerror("Помилка", "Немає позицій")
            return
        try:
            for line in lines:
                client_id = self.get_item_client(line["item_id"])
                if not client_id:
                    raise ValueError("Невірний клієнт для товару")
                self.service.create_stock_move(
                    "WRITE_OFF",
                    "WRITEOFF",
                    writeoff_id,
                    client_id,
                    writeoff["warehouse_id"],
                    line["item_id"],
                    -float(line["qty"]),
                    self.current_user["username"],
                )
            self.db.execute("UPDATE writeoffs SET status = ? WHERE id = ?", ("Проведено", writeoff_id))
            self.db.commit()
            self.refresh_current()
        except Exception as exc:
            self.db.rollback()
            messagebox.showerror("Помилка", str(exc))


def init_defaults(db: DBManager) -> None:
    if db.execute("SELECT COUNT(*) as cnt FROM roles").fetchone()["cnt"] > 0:
        return
    role_admin = generate_id()
    roles = [
        (role_admin, "Адміністратор"),
        (generate_id(), "Комірник"),
        (generate_id(), "Супервайзер складу"),
        (generate_id(), "Менеджер логістики"),
        (generate_id(), "Клієнт"),
        (generate_id(), "Бухгалтер"),
    ]
    db.executemany("INSERT INTO roles (id, name) VALUES (?, ?)", roles)
    permissions = [
        (generate_id(), role_admin, "all"),
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
        (generate_id(), "admin", "Адміністратор", role_admin, salt, pw_hash, now_str()),
    )
    db.commit()


def handle_exception(exc_type, exc, tb):
    logging.error("Unhandled exception: %s", "".join(traceback.format_exception(exc_type, exc, tb)))
    messagebox.showerror("Помилка", f"Непередбачена помилка: {exc}")


def main() -> None:
    tk.Tk.report_callback_exception = staticmethod(handle_exception)
    db = DBManager()
    db.init_schema()
    init_defaults(db)
    dal = DAL(db)
    rbac = RBAC(dal)
    rbac.load()
    root = tk.Tk()
    root.withdraw()
    login = LoginDialog(root, dal)
    root.wait_window(login)
    if not login.result:
        return
    root.destroy()
    service = WMSService(db, dal)
    app = WMSApp(db, dal, service, rbac, login.result)
    app.mainloop()


if __name__ == "__main__":
    main()
