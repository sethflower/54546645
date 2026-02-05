#!/usr/bin/env python3
"""Warehouse management system for 3PL operators.

Features:
- Manage clients, warehouses, and products
- Inbound and outbound orders
- Inventory tracking per warehouse
- Stock movements audit trail
- Reports for inventory and movements
"""

from __future__ import annotations

import datetime as dt
import os
import sqlite3
import tkinter as tk
from tkinter import messagebox, ttk
from dataclasses import dataclass
from typing import Iterable, Optional


DB_PATH_DEFAULT = "warehouse.db"


@dataclass(frozen=True)
class Client:
    id: int
    name: str


@dataclass(frozen=True)
class Warehouse:
    id: int
    name: str
    location: str


@dataclass(frozen=True)
class Product:
    id: int
    sku: str
    name: str
    unit: str


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS clients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS warehouses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            location TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            unit TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS inventory (
            warehouse_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (warehouse_id, product_id),
            FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS inbound_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            warehouse_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (warehouse_id) REFERENCES warehouses(id)
        );

        CREATE TABLE IF NOT EXISTS inbound_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inbound_order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity REAL NOT NULL,
            lot TEXT,
            serial TEXT,
            expiry TEXT,
            location_id INTEGER,
            FOREIGN KEY (inbound_order_id) REFERENCES inbound_orders(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS outbound_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            warehouse_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (warehouse_id) REFERENCES warehouses(id)
        );

        CREATE TABLE IF NOT EXISTS outbound_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            outbound_order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity REAL NOT NULL,
            lot TEXT,
            serial TEXT,
            expiry TEXT,
            location_id INTEGER,
            FOREIGN KEY (outbound_order_id) REFERENCES outbound_orders(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS stock_movements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            warehouse_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity_change REAL NOT NULL,
            reason TEXT NOT NULL,
            ref_type TEXT NOT NULL,
            ref_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            warehouse_id INTEGER NOT NULL,
            zone TEXT NOT NULL,
            code TEXT NOT NULL,
            description TEXT,
            UNIQUE(warehouse_id, zone, code),
            FOREIGN KEY (warehouse_id) REFERENCES warehouses(id)
        );

        CREATE TABLE IF NOT EXISTS stock_lots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            warehouse_id INTEGER NOT NULL,
            location_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            lot TEXT,
            serial TEXT,
            expiry TEXT,
            quantity REAL NOT NULL,
            FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
            FOREIGN KEY (location_id) REFERENCES locations(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS storage_moves (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_location_id INTEGER NOT NULL,
            to_location_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            lot TEXT,
            serial TEXT,
            expiry TEXT,
            quantity REAL NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (from_location_id) REFERENCES locations(id),
            FOREIGN KEY (to_location_id) REFERENCES locations(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS inventory_adjustments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            lot TEXT,
            serial TEXT,
            expiry TEXT,
            quantity_change REAL NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (location_id) REFERENCES locations(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        );

        CREATE TABLE IF NOT EXISTS roles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS permissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS role_permissions (
            role_id INTEGER NOT NULL,
            permission_id INTEGER NOT NULL,
            PRIMARY KEY (role_id, permission_id),
            FOREIGN KEY (role_id) REFERENCES roles(id),
            FOREIGN KEY (permission_id) REFERENCES permissions(id)
        );

        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            full_name TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS user_roles (
            user_id INTEGER NOT NULL,
            role_id INTEGER NOT NULL,
            PRIMARY KEY (user_id, role_id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (role_id) REFERENCES roles(id)
        );

        CREATE TABLE IF NOT EXISTS billing_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            storage_rate REAL NOT NULL,
            handling_rate REAL NOT NULL,
            currency TEXT NOT NULL,
            effective_from TEXT NOT NULL,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        CREATE TABLE IF NOT EXISTS billing_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            service_type TEXT NOT NULL,
            quantity REAL NOT NULL,
            unit_price REAL NOT NULL,
            amount REAL NOT NULL,
            period TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (client_id) REFERENCES clients(id)
        );
        """
    )
    conn.commit()


def now_ts() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def add_client(conn: sqlite3.Connection, name: str) -> int:
    cursor = conn.execute("INSERT INTO clients (name) VALUES (?)", (name,))
    conn.commit()
    return int(cursor.lastrowid)


def add_warehouse(conn: sqlite3.Connection, name: str, location: str) -> int:
    cursor = conn.execute(
        "INSERT INTO warehouses (name, location) VALUES (?, ?)",
        (name, location),
    )
    conn.commit()
    return int(cursor.lastrowid)


def add_product(conn: sqlite3.Connection, sku: str, name: str, unit: str) -> int:
    cursor = conn.execute(
        "INSERT INTO products (sku, name, unit) VALUES (?, ?, ?)",
        (sku, name, unit),
    )
    conn.commit()
    return int(cursor.lastrowid)


def ensure_inventory_row(
    conn: sqlite3.Connection, warehouse_id: int, product_id: int
) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO inventory (warehouse_id, product_id, quantity) VALUES (?, ?, 0)",
        (warehouse_id, product_id),
    )


def create_inbound_order(
    conn: sqlite3.Connection, client_id: int, warehouse_id: int
) -> int:
    cursor = conn.execute(
        "INSERT INTO inbound_orders (client_id, warehouse_id, status, created_at) VALUES (?, ?, ?, ?)",
        (client_id, warehouse_id, "created", now_ts()),
    )
    conn.commit()
    return int(cursor.lastrowid)


def add_inbound_item(
    conn: sqlite3.Connection,
    inbound_order_id: int,
    product_id: int,
    quantity: float,
    lot: Optional[str] = None,
    serial: Optional[str] = None,
    expiry: Optional[str] = None,
    location_id: Optional[int] = None,
) -> int:
    cursor = conn.execute(
        "INSERT INTO inbound_items (inbound_order_id, product_id, quantity, lot, serial, expiry, location_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (inbound_order_id, product_id, quantity, lot, serial, expiry, location_id),
    )
    conn.commit()
    return int(cursor.lastrowid)


def receive_inbound_order(conn: sqlite3.Connection, inbound_order_id: int) -> None:
    order = conn.execute(
        "SELECT id, warehouse_id, status FROM inbound_orders WHERE id = ?",
        (inbound_order_id,),
    ).fetchone()
    if order is None:
        raise ValueError("Inbound order not found")
    if order["status"] == "received":
        raise ValueError("Inbound order already received")

    items = conn.execute(
        "SELECT product_id, quantity, lot, serial, expiry, location_id "
        "FROM inbound_items WHERE inbound_order_id = ?",
        (inbound_order_id,),
    ).fetchall()
    if not items:
        raise ValueError("Inbound order has no items")

    for item in items:
        if item["location_id"] is None:
            raise ValueError("Inbound item requires location_id for storage")
        ensure_inventory_row(conn, order["warehouse_id"], item["product_id"])
        conn.execute(
            "UPDATE inventory SET quantity = quantity + ? WHERE warehouse_id = ? AND product_id = ?",
            (item["quantity"], order["warehouse_id"], item["product_id"]),
        )
        adjust_stock_lot(
            conn,
            order["warehouse_id"],
            item["location_id"],
            item["product_id"],
            item["lot"],
            item["serial"],
            item["expiry"],
            item["quantity"],
        )
        conn.execute(
            "INSERT INTO stock_movements (warehouse_id, product_id, quantity_change, reason, ref_type, ref_id, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                order["warehouse_id"],
                item["product_id"],
                item["quantity"],
                "Inbound receipt",
                "inbound_order",
                inbound_order_id,
                now_ts(),
            ),
        )

    conn.execute(
        "UPDATE inbound_orders SET status = ? WHERE id = ?",
        ("received", inbound_order_id),
    )
    conn.commit()


def create_outbound_order(
    conn: sqlite3.Connection, client_id: int, warehouse_id: int
) -> int:
    cursor = conn.execute(
        "INSERT INTO outbound_orders (client_id, warehouse_id, status, created_at) VALUES (?, ?, ?, ?)",
        (client_id, warehouse_id, "created", now_ts()),
    )
    conn.commit()
    return int(cursor.lastrowid)


def add_outbound_item(
    conn: sqlite3.Connection,
    outbound_order_id: int,
    product_id: int,
    quantity: float,
    lot: Optional[str] = None,
    serial: Optional[str] = None,
    expiry: Optional[str] = None,
    location_id: Optional[int] = None,
) -> int:
    cursor = conn.execute(
        "INSERT INTO outbound_items (outbound_order_id, product_id, quantity, lot, serial, expiry, location_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (outbound_order_id, product_id, quantity, lot, serial, expiry, location_id),
    )
    conn.commit()
    return int(cursor.lastrowid)


def ship_outbound_order(conn: sqlite3.Connection, outbound_order_id: int) -> None:
    order = conn.execute(
        "SELECT id, warehouse_id, status FROM outbound_orders WHERE id = ?",
        (outbound_order_id,),
    ).fetchone()
    if order is None:
        raise ValueError("Outbound order not found")
    if order["status"] == "shipped":
        raise ValueError("Outbound order already shipped")

    items = conn.execute(
        "SELECT product_id, quantity, lot, serial, expiry, location_id "
        "FROM outbound_items WHERE outbound_order_id = ?",
        (outbound_order_id,),
    ).fetchall()
    if not items:
        raise ValueError("Outbound order has no items")

    for item in items:
        if item["location_id"] is None:
            raise ValueError("Outbound item requires location_id for picking")
        row = conn.execute(
            "SELECT quantity FROM stock_lots "
            "WHERE warehouse_id = ? AND location_id = ? AND product_id = ? "
            "AND COALESCE(lot, '') = COALESCE(?, '') "
            "AND COALESCE(serial, '') = COALESCE(?, '') "
            "AND COALESCE(expiry, '') = COALESCE(?, '')",
            (
                order["warehouse_id"],
                item["location_id"],
                item["product_id"],
                item["lot"],
                item["serial"],
                item["expiry"],
            ),
        ).fetchone()
        available = row["quantity"] if row else 0
        if available < item["quantity"]:
            raise ValueError(
                f"Insufficient stock for product {item['product_id']}: {available} available"
            )

    for item in items:
        conn.execute(
            "UPDATE inventory SET quantity = quantity - ? WHERE warehouse_id = ? AND product_id = ?",
            (item["quantity"], order["warehouse_id"], item["product_id"]),
        )
        adjust_stock_lot(
            conn,
            order["warehouse_id"],
            item["location_id"],
            item["product_id"],
            item["lot"],
            item["serial"],
            item["expiry"],
            -item["quantity"],
        )
        conn.execute(
            "INSERT INTO stock_movements (warehouse_id, product_id, quantity_change, reason, ref_type, ref_id, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                order["warehouse_id"],
                item["product_id"],
                -item["quantity"],
                "Outbound shipment",
                "outbound_order",
                outbound_order_id,
                now_ts(),
            ),
        )

    conn.execute(
        "UPDATE outbound_orders SET status = ? WHERE id = ?",
        ("shipped", outbound_order_id),
    )
    conn.commit()


def fetch_all(conn: sqlite3.Connection, query: str, params: Iterable[object] = ()) -> list[sqlite3.Row]:
    return list(conn.execute(query, tuple(params)).fetchall())


def list_clients(conn: sqlite3.Connection) -> list[Client]:
    return [Client(id=row["id"], name=row["name"]) for row in fetch_all(conn, "SELECT id, name FROM clients")]


def list_warehouses(conn: sqlite3.Connection) -> list[Warehouse]:
    return [
        Warehouse(id=row["id"], name=row["name"], location=row["location"])
        for row in fetch_all(conn, "SELECT id, name, location FROM warehouses")
    ]


def list_products(conn: sqlite3.Connection) -> list[Product]:
    return [
        Product(id=row["id"], sku=row["sku"], name=row["name"], unit=row["unit"])
        for row in fetch_all(conn, "SELECT id, sku, name, unit FROM products")
    ]


def list_locations(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return fetch_all(
        conn,
        "SELECT l.id, l.zone, l.code, l.description, w.name AS warehouse "
        "FROM locations l JOIN warehouses w ON w.id = l.warehouse_id "
        "ORDER BY w.name, l.zone, l.code",
    )


def add_location(
    conn: sqlite3.Connection, warehouse_id: int, zone: str, code: str, description: str
) -> int:
    cursor = conn.execute(
        "INSERT INTO locations (warehouse_id, zone, code, description) VALUES (?, ?, ?, ?)",
        (warehouse_id, zone, code, description),
    )
    conn.commit()
    return int(cursor.lastrowid)


def update_location(
    conn: sqlite3.Connection, location_id: int, zone: str, code: str, description: str
) -> None:
    conn.execute(
        "UPDATE locations SET zone = ?, code = ?, description = ? WHERE id = ?",
        (zone, code, description, location_id),
    )
    conn.commit()


def delete_location(conn: sqlite3.Connection, location_id: int) -> None:
    conn.execute("DELETE FROM locations WHERE id = ?", (location_id,))
    conn.commit()


def list_roles(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return fetch_all(conn, "SELECT id, name, description FROM roles ORDER BY name")


def add_role(conn: sqlite3.Connection, name: str, description: str) -> int:
    cursor = conn.execute("INSERT INTO roles (name, description) VALUES (?, ?)", (name, description))
    conn.commit()
    return int(cursor.lastrowid)


def update_role(conn: sqlite3.Connection, role_id: int, name: str, description: str) -> None:
    conn.execute("UPDATE roles SET name = ?, description = ? WHERE id = ?", (name, description, role_id))
    conn.commit()


def delete_role(conn: sqlite3.Connection, role_id: int) -> None:
    conn.execute("DELETE FROM roles WHERE id = ?", (role_id,))
    conn.commit()


def list_permissions(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return fetch_all(conn, "SELECT id, code, description FROM permissions ORDER BY code")


def add_permission(conn: sqlite3.Connection, code: str, description: str) -> int:
    cursor = conn.execute(
        "INSERT INTO permissions (code, description) VALUES (?, ?)",
        (code, description),
    )
    conn.commit()
    return int(cursor.lastrowid)


def set_role_permissions(conn: sqlite3.Connection, role_id: int, permission_ids: list[int]) -> None:
    conn.execute("DELETE FROM role_permissions WHERE role_id = ?", (role_id,))
    conn.executemany(
        "INSERT INTO role_permissions (role_id, permission_id) VALUES (?, ?)",
        [(role_id, perm_id) for perm_id in permission_ids],
    )
    conn.commit()


def list_users(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return fetch_all(conn, "SELECT id, username, full_name, is_active FROM users ORDER BY username")


def add_user(conn: sqlite3.Connection, username: str, full_name: str, is_active: bool) -> int:
    cursor = conn.execute(
        "INSERT INTO users (username, full_name, is_active) VALUES (?, ?, ?)",
        (username, full_name, 1 if is_active else 0),
    )
    conn.commit()
    return int(cursor.lastrowid)


def update_user(conn: sqlite3.Connection, user_id: int, username: str, full_name: str, is_active: bool) -> None:
    conn.execute(
        "UPDATE users SET username = ?, full_name = ?, is_active = ? WHERE id = ?",
        (username, full_name, 1 if is_active else 0, user_id),
    )
    conn.commit()


def delete_user(conn: sqlite3.Connection, user_id: int) -> None:
    conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()


def set_user_roles(conn: sqlite3.Connection, user_id: int, role_ids: list[int]) -> None:
    conn.execute("DELETE FROM user_roles WHERE user_id = ?", (user_id,))
    conn.executemany(
        "INSERT INTO user_roles (user_id, role_id) VALUES (?, ?)",
        [(user_id, role_id) for role_id in role_ids],
    )
    conn.commit()


def list_stock_lots(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return fetch_all(
        conn,
        "SELECT s.id, w.name AS warehouse, l.zone, l.code, p.sku, p.name AS product, "
        "s.lot, s.serial, s.expiry, s.quantity "
        "FROM stock_lots s "
        "JOIN warehouses w ON w.id = s.warehouse_id "
        "JOIN locations l ON l.id = s.location_id "
        "JOIN products p ON p.id = s.product_id "
        "ORDER BY w.name, l.zone, l.code, p.sku",
    )


def adjust_stock_lot(
    conn: sqlite3.Connection,
    warehouse_id: int,
    location_id: int,
    product_id: int,
    lot: Optional[str],
    serial: Optional[str],
    expiry: Optional[str],
    quantity_change: float,
) -> None:
    existing = conn.execute(
        "SELECT id, quantity FROM stock_lots "
        "WHERE warehouse_id = ? AND location_id = ? AND product_id = ? "
        "AND COALESCE(lot, '') = COALESCE(?, '') "
        "AND COALESCE(serial, '') = COALESCE(?, '') "
        "AND COALESCE(expiry, '') = COALESCE(?, '')",
        (warehouse_id, location_id, product_id, lot, serial, expiry),
    ).fetchone()
    if existing:
        conn.execute(
            "UPDATE stock_lots SET quantity = quantity + ? WHERE id = ?",
            (quantity_change, existing["id"]),
        )
    else:
        conn.execute(
            "INSERT INTO stock_lots (warehouse_id, location_id, product_id, lot, serial, expiry, quantity) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (warehouse_id, location_id, product_id, lot, serial, expiry, quantity_change),
        )
    conn.commit()


def add_inventory_adjustment(
    conn: sqlite3.Connection,
    location_id: int,
    product_id: int,
    lot: Optional[str],
    serial: Optional[str],
    expiry: Optional[str],
    quantity_change: float,
    reason: str,
) -> None:
    conn.execute(
        "INSERT INTO inventory_adjustments "
        "(location_id, product_id, lot, serial, expiry, quantity_change, reason, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (location_id, product_id, lot, serial, expiry, quantity_change, reason, now_ts()),
    )
    conn.commit()


def add_storage_move(
    conn: sqlite3.Connection,
    from_location_id: int,
    to_location_id: int,
    product_id: int,
    lot: Optional[str],
    serial: Optional[str],
    expiry: Optional[str],
    quantity: float,
    reason: str,
) -> None:
    conn.execute(
        "INSERT INTO storage_moves "
        "(from_location_id, to_location_id, product_id, lot, serial, expiry, quantity, reason, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (from_location_id, to_location_id, product_id, lot, serial, expiry, quantity, reason, now_ts()),
    )
    conn.commit()


def add_billing_rate(
    conn: sqlite3.Connection,
    client_id: int,
    storage_rate: float,
    handling_rate: float,
    currency: str,
    effective_from: str,
) -> None:
    conn.execute(
        "INSERT INTO billing_rates (client_id, storage_rate, handling_rate, currency, effective_from) "
        "VALUES (?, ?, ?, ?, ?)",
        (client_id, storage_rate, handling_rate, currency, effective_from),
    )
    conn.commit()


def list_billing_rates(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return fetch_all(
        conn,
        "SELECT b.id, c.name AS client, b.storage_rate, b.handling_rate, b.currency, b.effective_from "
        "FROM billing_rates b JOIN clients c ON c.id = b.client_id ORDER BY b.effective_from DESC",
    )


def add_billing_record(
    conn: sqlite3.Connection,
    client_id: int,
    service_type: str,
    quantity: float,
    unit_price: float,
    period: str,
) -> None:
    amount = quantity * unit_price
    conn.execute(
        "INSERT INTO billing_records "
        "(client_id, service_type, quantity, unit_price, amount, period, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (client_id, service_type, quantity, unit_price, amount, period, now_ts()),
    )
    conn.commit()


def list_billing_records(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return fetch_all(
        conn,
        "SELECT r.id, c.name AS client, r.service_type, r.quantity, r.unit_price, r.amount, r.period, r.created_at "
        "FROM billing_records r JOIN clients c ON c.id = r.client_id ORDER BY r.created_at DESC",
    )


def inventory_report(conn: sqlite3.Connection, warehouse_id: Optional[int] = None) -> list[sqlite3.Row]:
    base = (
        "SELECT w.name AS warehouse, p.sku, p.name AS product, p.unit, i.quantity "
        "FROM inventory i "
        "JOIN warehouses w ON w.id = i.warehouse_id "
        "JOIN products p ON p.id = i.product_id"
    )
    params: list[object] = []
    if warehouse_id is not None:
        base += " WHERE i.warehouse_id = ?"
        params.append(warehouse_id)
    base += " ORDER BY w.name, p.sku"
    return fetch_all(conn, base, params)


def movement_report(conn: sqlite3.Connection, limit: int = 50) -> list[sqlite3.Row]:
    return fetch_all(
        conn,
        "SELECT m.created_at, w.name AS warehouse, p.sku, p.name AS product, m.quantity_change, m.reason, m.ref_type, m.ref_id "
        "FROM stock_movements m "
        "JOIN warehouses w ON w.id = m.warehouse_id "
        "JOIN products p ON p.id = m.product_id "
        "ORDER BY m.created_at DESC LIMIT ?",
        (limit,),
    )


def get_db_path() -> str:
    return os.environ.get("WAREHOUSE_DB", DB_PATH_DEFAULT)

class WarehouseApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("3PL Warehouse Management System")
        self.geometry("1200x720")
        self.conn = connect(get_db_path())
        init_db(self.conn)

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        self._build_dashboard()
        self._build_clients_tab()
        self._build_warehouses_tab()
        self._build_locations_tab()
        self._build_products_tab()
        self._build_inbound_tab()
        self._build_outbound_tab()
        self._build_inventory_tab()
        self._build_adjustments_tab()
        self._build_storage_moves_tab()
        self._build_movements_tab()
        self._build_reports_tab()
        self._build_finance_tab()
        self._build_admin_tab()
        self._refresh_comboboxes()

    def _build_dashboard(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Обзор")

        self.dashboard_stats = ttk.Label(frame, text="", font=("Segoe UI", 12))
        self.dashboard_stats.pack(anchor="w", padx=16, pady=16)

        self.dashboard_inventory = self._make_tree(
            frame,
            ["Склад", "SKU", "Товар", "Ед.", "Кол-во"],
        )
        self.dashboard_inventory.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)

        self.dashboard_movements = self._make_tree(
            frame,
            ["Дата", "Склад", "SKU", "Товар", "Изменение", "Причина"],
        )
        self.dashboard_movements.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)

        self._refresh_dashboard()

    def _build_clients_tab(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Клиенты")

        form = ttk.Frame(frame)
        form.pack(fill=tk.X, padx=16, pady=8)
        ttk.Label(form, text="Название").grid(row=0, column=0, padx=4, pady=4, sticky="w")
        self.client_name = ttk.Entry(form, width=40)
        self.client_name.grid(row=0, column=1, padx=4, pady=4)
        ttk.Button(form, text="Добавить", command=self._add_client).grid(row=0, column=2, padx=4, pady=4)
        ttk.Button(form, text="Обновить", command=self._refresh_clients).grid(row=0, column=3, padx=4, pady=4)

        self.clients_tree = self._make_tree(frame, ["ID", "Название"])
        self.clients_tree.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)

        actions = ttk.Frame(frame)
        actions.pack(fill=tk.X, padx=16, pady=8)
        ttk.Button(actions, text="Редактировать", command=self._edit_client).pack(side=tk.LEFT, padx=4)
        ttk.Button(actions, text="Удалить", command=self._delete_client).pack(side=tk.LEFT, padx=4)

        self._refresh_clients()

    def _build_warehouses_tab(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Склады")

        form = ttk.Frame(frame)
        form.pack(fill=tk.X, padx=16, pady=8)
        ttk.Label(form, text="Название").grid(row=0, column=0, padx=4, pady=4, sticky="w")
        self.warehouse_name = ttk.Entry(form, width=30)
        self.warehouse_name.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(form, text="Локация").grid(row=0, column=2, padx=4, pady=4, sticky="w")
        self.warehouse_location = ttk.Entry(form, width=30)
        self.warehouse_location.grid(row=0, column=3, padx=4, pady=4)
        ttk.Button(form, text="Добавить", command=self._add_warehouse).grid(row=0, column=4, padx=4, pady=4)
        ttk.Button(form, text="Обновить", command=self._refresh_warehouses).grid(row=0, column=5, padx=4, pady=4)

        self.warehouses_tree = self._make_tree(frame, ["ID", "Название", "Локация"])
        self.warehouses_tree.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)

        actions = ttk.Frame(frame)
        actions.pack(fill=tk.X, padx=16, pady=8)
        ttk.Button(actions, text="Редактировать", command=self._edit_warehouse).pack(side=tk.LEFT, padx=4)
        ttk.Button(actions, text="Удалить", command=self._delete_warehouse).pack(side=tk.LEFT, padx=4)

        self._refresh_warehouses()

    def _build_locations_tab(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Локации")

        form = ttk.Frame(frame)
        form.pack(fill=tk.X, padx=16, pady=8)
        ttk.Label(form, text="Склад").grid(row=0, column=0, padx=4, pady=4, sticky="w")
        self.location_warehouse = ttk.Combobox(form, values=self._warehouse_names(), width=30, state="readonly")
        self.location_warehouse.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(form, text="Зона").grid(row=0, column=2, padx=4, pady=4, sticky="w")
        self.location_zone = ttk.Entry(form, width=15)
        self.location_zone.grid(row=0, column=3, padx=4, pady=4)
        ttk.Label(form, text="Код").grid(row=0, column=4, padx=4, pady=4, sticky="w")
        self.location_code = ttk.Entry(form, width=15)
        self.location_code.grid(row=0, column=5, padx=4, pady=4)
        ttk.Label(form, text="Описание").grid(row=0, column=6, padx=4, pady=4, sticky="w")
        self.location_description = ttk.Entry(form, width=30)
        self.location_description.grid(row=0, column=7, padx=4, pady=4)
        ttk.Button(form, text="Добавить", command=self._add_location).grid(row=0, column=8, padx=4, pady=4)
        ttk.Button(form, text="Обновить", command=self._refresh_locations).grid(row=0, column=9, padx=4, pady=4)

        self.locations_tree = self._make_tree(frame, ["ID", "Склад", "Зона", "Код", "Описание"])
        self.locations_tree.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)

        actions = ttk.Frame(frame)
        actions.pack(fill=tk.X, padx=16, pady=8)
        ttk.Button(actions, text="Редактировать", command=self._edit_location).pack(side=tk.LEFT, padx=4)
        ttk.Button(actions, text="Удалить", command=self._delete_location).pack(side=tk.LEFT, padx=4)

        self._refresh_locations()

    def _build_products_tab(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Товары")

        form = ttk.Frame(frame)
        form.pack(fill=tk.X, padx=16, pady=8)
        ttk.Label(form, text="SKU").grid(row=0, column=0, padx=4, pady=4, sticky="w")
        self.product_sku = ttk.Entry(form, width=20)
        self.product_sku.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(form, text="Название").grid(row=0, column=2, padx=4, pady=4, sticky="w")
        self.product_name = ttk.Entry(form, width=30)
        self.product_name.grid(row=0, column=3, padx=4, pady=4)
        ttk.Label(form, text="Ед.").grid(row=0, column=4, padx=4, pady=4, sticky="w")
        self.product_unit = ttk.Entry(form, width=10)
        self.product_unit.grid(row=0, column=5, padx=4, pady=4)
        ttk.Button(form, text="Добавить", command=self._add_product).grid(row=0, column=6, padx=4, pady=4)
        ttk.Button(form, text="Обновить", command=self._refresh_products).grid(row=0, column=7, padx=4, pady=4)

        self.products_tree = self._make_tree(frame, ["ID", "SKU", "Название", "Ед."])
        self.products_tree.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)

        actions = ttk.Frame(frame)
        actions.pack(fill=tk.X, padx=16, pady=8)
        ttk.Button(actions, text="Редактировать", command=self._edit_product).pack(side=tk.LEFT, padx=4)
        ttk.Button(actions, text="Удалить", command=self._delete_product).pack(side=tk.LEFT, padx=4)

        self._refresh_products()

    def _build_inbound_tab(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Приход")

        form = ttk.Frame(frame)
        form.pack(fill=tk.X, padx=16, pady=8)
        ttk.Label(form, text="Клиент").grid(row=0, column=0, padx=4, pady=4, sticky="w")
        self.inbound_client = ttk.Combobox(form, values=self._client_names(), width=30, state="readonly")
        self.inbound_client.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(form, text="Склад").grid(row=0, column=2, padx=4, pady=4, sticky="w")
        self.inbound_warehouse = ttk.Combobox(form, values=self._warehouse_names(), width=30, state="readonly")
        self.inbound_warehouse.grid(row=0, column=3, padx=4, pady=4)
        ttk.Button(form, text="Создать приход", command=self._create_inbound).grid(row=0, column=4, padx=4, pady=4)

        self.inbound_orders_tree = self._make_tree(frame, ["ID", "Клиент", "Склад", "Статус", "Создан"])
        self.inbound_orders_tree.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)

        item_form = ttk.Frame(frame)
        item_form.pack(fill=tk.X, padx=16, pady=8)
        ttk.Label(item_form, text="Заказ ID").grid(row=0, column=0, padx=4, pady=4)
        self.inbound_order_id = ttk.Entry(item_form, width=10)
        self.inbound_order_id.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(item_form, text="Товар").grid(row=0, column=2, padx=4, pady=4)
        self.inbound_product = ttk.Combobox(item_form, values=self._product_names(), width=30, state="readonly")
        self.inbound_product.grid(row=0, column=3, padx=4, pady=4)
        ttk.Label(item_form, text="Локация").grid(row=0, column=4, padx=4, pady=4)
        self.inbound_location = ttk.Combobox(item_form, values=self._location_names(), width=20, state="readonly")
        self.inbound_location.grid(row=0, column=5, padx=4, pady=4)
        ttk.Label(item_form, text="Партия").grid(row=0, column=6, padx=4, pady=4)
        self.inbound_lot = ttk.Entry(item_form, width=12)
        self.inbound_lot.grid(row=0, column=7, padx=4, pady=4)
        ttk.Label(item_form, text="Серия").grid(row=0, column=8, padx=4, pady=4)
        self.inbound_serial = ttk.Entry(item_form, width=12)
        self.inbound_serial.grid(row=0, column=9, padx=4, pady=4)
        ttk.Label(item_form, text="Срок годн.").grid(row=0, column=10, padx=4, pady=4)
        self.inbound_expiry = ttk.Entry(item_form, width=12)
        self.inbound_expiry.grid(row=0, column=11, padx=4, pady=4)
        ttk.Label(item_form, text="Количество").grid(row=0, column=12, padx=4, pady=4)
        self.inbound_quantity = ttk.Entry(item_form, width=10)
        self.inbound_quantity.grid(row=0, column=13, padx=4, pady=4)
        ttk.Button(item_form, text="Добавить позицию", command=self._add_inbound_item).grid(row=0, column=14, padx=4, pady=4)
        ttk.Button(item_form, text="Принять", command=self._receive_inbound).grid(row=0, column=15, padx=4, pady=4)

        self._refresh_inbound_orders()

    def _build_outbound_tab(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Отгрузка")

        form = ttk.Frame(frame)
        form.pack(fill=tk.X, padx=16, pady=8)
        ttk.Label(form, text="Клиент").grid(row=0, column=0, padx=4, pady=4)
        self.outbound_client = ttk.Combobox(form, values=self._client_names(), width=30, state="readonly")
        self.outbound_client.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(form, text="Склад").grid(row=0, column=2, padx=4, pady=4)
        self.outbound_warehouse = ttk.Combobox(form, values=self._warehouse_names(), width=30, state="readonly")
        self.outbound_warehouse.grid(row=0, column=3, padx=4, pady=4)
        ttk.Button(form, text="Создать отгрузку", command=self._create_outbound).grid(row=0, column=4, padx=4, pady=4)

        self.outbound_orders_tree = self._make_tree(frame, ["ID", "Клиент", "Склад", "Статус", "Создан"])
        self.outbound_orders_tree.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)

        item_form = ttk.Frame(frame)
        item_form.pack(fill=tk.X, padx=16, pady=8)
        ttk.Label(item_form, text="Заказ ID").grid(row=0, column=0, padx=4, pady=4)
        self.outbound_order_id = ttk.Entry(item_form, width=10)
        self.outbound_order_id.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(item_form, text="Товар").grid(row=0, column=2, padx=4, pady=4)
        self.outbound_product = ttk.Combobox(item_form, values=self._product_names(), width=30, state="readonly")
        self.outbound_product.grid(row=0, column=3, padx=4, pady=4)
        ttk.Label(item_form, text="Локация").grid(row=0, column=4, padx=4, pady=4)
        self.outbound_location = ttk.Combobox(item_form, values=self._location_names(), width=20, state="readonly")
        self.outbound_location.grid(row=0, column=5, padx=4, pady=4)
        ttk.Label(item_form, text="Партия").grid(row=0, column=6, padx=4, pady=4)
        self.outbound_lot = ttk.Entry(item_form, width=12)
        self.outbound_lot.grid(row=0, column=7, padx=4, pady=4)
        ttk.Label(item_form, text="Серия").grid(row=0, column=8, padx=4, pady=4)
        self.outbound_serial = ttk.Entry(item_form, width=12)
        self.outbound_serial.grid(row=0, column=9, padx=4, pady=4)
        ttk.Label(item_form, text="Срок годн.").grid(row=0, column=10, padx=4, pady=4)
        self.outbound_expiry = ttk.Entry(item_form, width=12)
        self.outbound_expiry.grid(row=0, column=11, padx=4, pady=4)
        ttk.Label(item_form, text="Количество").grid(row=0, column=12, padx=4, pady=4)
        self.outbound_quantity = ttk.Entry(item_form, width=10)
        self.outbound_quantity.grid(row=0, column=13, padx=4, pady=4)
        ttk.Button(item_form, text="Добавить позицию", command=self._add_outbound_item).grid(row=0, column=14, padx=4, pady=4)
        ttk.Button(item_form, text="Отгрузить", command=self._ship_outbound).grid(row=0, column=15, padx=4, pady=4)

        self._refresh_outbound_orders()

    def _build_inventory_tab(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Остатки")

        filter_frame = ttk.Frame(frame)
        filter_frame.pack(fill=tk.X, padx=16, pady=8)
        ttk.Label(filter_frame, text="Склад").grid(row=0, column=0, padx=4, pady=4)
        self.inventory_warehouse = ttk.Combobox(
            filter_frame, values=["Все"] + self._warehouse_names(), width=30, state="readonly"
        )
        self.inventory_warehouse.grid(row=0, column=1, padx=4, pady=4)
        self.inventory_warehouse.set("Все")
        ttk.Button(filter_frame, text="Показать", command=self._refresh_inventory).grid(
            row=0, column=2, padx=4, pady=4
        )

        self.inventory_tree = self._make_tree(frame, ["Склад", "SKU", "Товар", "Ед.", "Кол-во"])
        self.inventory_tree.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)
        self._refresh_inventory()

    def _build_adjustments_tab(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Инвентаризация")

        form = ttk.Frame(frame)
        form.pack(fill=tk.X, padx=16, pady=8)
        ttk.Label(form, text="Локация").grid(row=0, column=0, padx=4, pady=4)
        self.adjust_location = ttk.Combobox(form, values=self._location_names(), width=30, state="readonly")
        self.adjust_location.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(form, text="Товар").grid(row=0, column=2, padx=4, pady=4)
        self.adjust_product = ttk.Combobox(form, values=self._product_names(), width=30, state="readonly")
        self.adjust_product.grid(row=0, column=3, padx=4, pady=4)
        ttk.Label(form, text="Партия").grid(row=0, column=4, padx=4, pady=4)
        self.adjust_lot = ttk.Entry(form, width=12)
        self.adjust_lot.grid(row=0, column=5, padx=4, pady=4)
        ttk.Label(form, text="Серия").grid(row=0, column=6, padx=4, pady=4)
        self.adjust_serial = ttk.Entry(form, width=12)
        self.adjust_serial.grid(row=0, column=7, padx=4, pady=4)
        ttk.Label(form, text="Срок годн.").grid(row=0, column=8, padx=4, pady=4)
        self.adjust_expiry = ttk.Entry(form, width=12)
        self.adjust_expiry.grid(row=0, column=9, padx=4, pady=4)
        ttk.Label(form, text="Изменение").grid(row=0, column=10, padx=4, pady=4)
        self.adjust_quantity = ttk.Entry(form, width=10)
        self.adjust_quantity.grid(row=0, column=11, padx=4, pady=4)
        ttk.Label(form, text="Причина").grid(row=0, column=12, padx=4, pady=4)
        self.adjust_reason = ttk.Entry(form, width=20)
        self.adjust_reason.grid(row=0, column=13, padx=4, pady=4)
        ttk.Button(form, text="Провести", command=self._apply_adjustment).grid(row=0, column=14, padx=4, pady=4)

        self.adjustments_tree = self._make_tree(
            frame,
            ["ID", "Локация", "SKU", "Товар", "Партия", "Серия", "Срок годн.", "Изменение", "Причина", "Дата"],
        )
        self.adjustments_tree.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)
        self._refresh_adjustments()

    def _build_storage_moves_tab(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Хранение")

        form = ttk.Frame(frame)
        form.pack(fill=tk.X, padx=16, pady=8)
        ttk.Label(form, text="Откуда").grid(row=0, column=0, padx=4, pady=4)
        self.move_from = ttk.Combobox(form, values=self._location_names(), width=30, state="readonly")
        self.move_from.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(form, text="Куда").grid(row=0, column=2, padx=4, pady=4)
        self.move_to = ttk.Combobox(form, values=self._location_names(), width=30, state="readonly")
        self.move_to.grid(row=0, column=3, padx=4, pady=4)
        ttk.Label(form, text="Товар").grid(row=0, column=4, padx=4, pady=4)
        self.move_product = ttk.Combobox(form, values=self._product_names(), width=30, state="readonly")
        self.move_product.grid(row=0, column=5, padx=4, pady=4)
        ttk.Label(form, text="Партия").grid(row=0, column=6, padx=4, pady=4)
        self.move_lot = ttk.Entry(form, width=12)
        self.move_lot.grid(row=0, column=7, padx=4, pady=4)
        ttk.Label(form, text="Серия").grid(row=0, column=8, padx=4, pady=4)
        self.move_serial = ttk.Entry(form, width=12)
        self.move_serial.grid(row=0, column=9, padx=4, pady=4)
        ttk.Label(form, text="Срок годн.").grid(row=0, column=10, padx=4, pady=4)
        self.move_expiry = ttk.Entry(form, width=12)
        self.move_expiry.grid(row=0, column=11, padx=4, pady=4)
        ttk.Label(form, text="Количество").grid(row=0, column=12, padx=4, pady=4)
        self.move_quantity = ttk.Entry(form, width=10)
        self.move_quantity.grid(row=0, column=13, padx=4, pady=4)
        ttk.Label(form, text="Причина").grid(row=0, column=14, padx=4, pady=4)
        self.move_reason = ttk.Entry(form, width=20)
        self.move_reason.grid(row=0, column=15, padx=4, pady=4)
        ttk.Button(form, text="Переместить", command=self._apply_move).grid(row=0, column=16, padx=4, pady=4)

        self.moves_tree = self._make_tree(
            frame,
            ["ID", "Откуда", "Куда", "SKU", "Товар", "Партия", "Серия", "Срок годн.", "Кол-во", "Причина", "Дата"],
        )
        self.moves_tree.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)
        self._refresh_moves()

    def _build_movements_tab(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Движения")

        filter_frame = ttk.Frame(frame)
        filter_frame.pack(fill=tk.X, padx=16, pady=8)
        ttk.Label(filter_frame, text="Лимит").grid(row=0, column=0, padx=4, pady=4)
        self.movements_limit = ttk.Entry(filter_frame, width=10)
        self.movements_limit.insert(0, "50")
        self.movements_limit.grid(row=0, column=1, padx=4, pady=4)
        ttk.Button(filter_frame, text="Показать", command=self._refresh_movements).grid(
            row=0, column=2, padx=4, pady=4
        )

        self.movements_tree = self._make_tree(
            frame, ["Дата", "Склад", "SKU", "Товар", "Изменение", "Причина", "Тип", "ID"]
        )
        self.movements_tree.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)
        self._refresh_movements()

    def _build_reports_tab(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Отчёты")

        self.reports_summary = ttk.Label(frame, text="", font=("Segoe UI", 11))
        self.reports_summary.pack(anchor="w", padx=16, pady=8)

        self.reports_inventory_tree = self._make_tree(
            frame,
            ["Склад", "Локация", "SKU", "Товар", "Партия", "Серия", "Срок годн.", "Кол-во"],
        )
        self.reports_inventory_tree.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)

        self.reports_movement_tree = self._make_tree(
            frame,
            ["Дата", "Склад", "SKU", "Товар", "Изменение", "Причина", "Тип", "ID"],
        )
        self.reports_movement_tree.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)
        self._refresh_reports()

    def _build_finance_tab(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Финансы 3PL")

        rate_form = ttk.Frame(frame)
        rate_form.pack(fill=tk.X, padx=16, pady=8)
        ttk.Label(rate_form, text="Клиент").grid(row=0, column=0, padx=4, pady=4)
        self.billing_client = ttk.Combobox(rate_form, values=self._client_names(), width=30, state="readonly")
        self.billing_client.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(rate_form, text="Ставка хранения").grid(row=0, column=2, padx=4, pady=4)
        self.billing_storage_rate = ttk.Entry(rate_form, width=10)
        self.billing_storage_rate.grid(row=0, column=3, padx=4, pady=4)
        ttk.Label(rate_form, text="Ставка обработки").grid(row=0, column=4, padx=4, pady=4)
        self.billing_handling_rate = ttk.Entry(rate_form, width=10)
        self.billing_handling_rate.grid(row=0, column=5, padx=4, pady=4)
        ttk.Label(rate_form, text="Валюта").grid(row=0, column=6, padx=4, pady=4)
        self.billing_currency = ttk.Entry(rate_form, width=8)
        self.billing_currency.insert(0, "RUB")
        self.billing_currency.grid(row=0, column=7, padx=4, pady=4)
        ttk.Label(rate_form, text="С даты").grid(row=0, column=8, padx=4, pady=4)
        self.billing_effective = ttk.Entry(rate_form, width=12)
        self.billing_effective.grid(row=0, column=9, padx=4, pady=4)
        ttk.Button(rate_form, text="Добавить тариф", command=self._add_billing_rate).grid(
            row=0, column=10, padx=4, pady=4
        )

        self.billing_rates_tree = self._make_tree(
            frame, ["ID", "Клиент", "Хранение", "Обработка", "Валюта", "С даты"]
        )
        self.billing_rates_tree.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)

        record_form = ttk.Frame(frame)
        record_form.pack(fill=tk.X, padx=16, pady=8)
        ttk.Label(record_form, text="Клиент").grid(row=0, column=0, padx=4, pady=4)
        self.charge_client = ttk.Combobox(record_form, values=self._client_names(), width=30, state="readonly")
        self.charge_client.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(record_form, text="Услуга").grid(row=0, column=2, padx=4, pady=4)
        self.charge_service = ttk.Entry(record_form, width=20)
        self.charge_service.grid(row=0, column=3, padx=4, pady=4)
        ttk.Label(record_form, text="Кол-во").grid(row=0, column=4, padx=4, pady=4)
        self.charge_quantity = ttk.Entry(record_form, width=10)
        self.charge_quantity.grid(row=0, column=5, padx=4, pady=4)
        ttk.Label(record_form, text="Цена").grid(row=0, column=6, padx=4, pady=4)
        self.charge_unit_price = ttk.Entry(record_form, width=10)
        self.charge_unit_price.grid(row=0, column=7, padx=4, pady=4)
        ttk.Label(record_form, text="Период").grid(row=0, column=8, padx=4, pady=4)
        self.charge_period = ttk.Entry(record_form, width=12)
        self.charge_period.grid(row=0, column=9, padx=4, pady=4)
        ttk.Button(record_form, text="Начислить", command=self._add_billing_record).grid(
            row=0, column=10, padx=4, pady=4
        )

        self.billing_records_tree = self._make_tree(
            frame, ["ID", "Клиент", "Услуга", "Кол-во", "Цена", "Сумма", "Период", "Дата"]
        )
        self.billing_records_tree.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)
        self._refresh_finance()

    def _build_admin_tab(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Администрирование")

        roles_frame = ttk.LabelFrame(frame, text="Роли")
        roles_frame.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)
        role_form = ttk.Frame(roles_frame)
        role_form.pack(fill=tk.X, padx=8, pady=4)
        ttk.Label(role_form, text="Название").grid(row=0, column=0, padx=4, pady=4)
        self.role_name = ttk.Entry(role_form, width=20)
        self.role_name.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(role_form, text="Описание").grid(row=0, column=2, padx=4, pady=4)
        self.role_description = ttk.Entry(role_form, width=30)
        self.role_description.grid(row=0, column=3, padx=4, pady=4)
        ttk.Button(role_form, text="Добавить", command=self._add_role).grid(row=0, column=4, padx=4, pady=4)
        ttk.Button(role_form, text="Обновить", command=self._refresh_roles).grid(row=0, column=5, padx=4, pady=4)
        self.roles_tree = self._make_tree(roles_frame, ["ID", "Название", "Описание"])
        self.roles_tree.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

        permissions_frame = ttk.LabelFrame(frame, text="Права")
        permissions_frame.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)
        perm_form = ttk.Frame(permissions_frame)
        perm_form.pack(fill=tk.X, padx=8, pady=4)
        ttk.Label(perm_form, text="Код").grid(row=0, column=0, padx=4, pady=4)
        self.permission_code = ttk.Entry(perm_form, width=20)
        self.permission_code.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(perm_form, text="Описание").grid(row=0, column=2, padx=4, pady=4)
        self.permission_description = ttk.Entry(perm_form, width=40)
        self.permission_description.grid(row=0, column=3, padx=4, pady=4)
        ttk.Button(perm_form, text="Добавить", command=self._add_permission).grid(row=0, column=4, padx=4, pady=4)
        self.permissions_tree = self._make_tree(permissions_frame, ["ID", "Код", "Описание"])
        self.permissions_tree.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

        role_perm_frame = ttk.Frame(permissions_frame)
        role_perm_frame.pack(fill=tk.X, padx=8, pady=4)
        ttk.Label(role_perm_frame, text="Роль").grid(row=0, column=0, padx=4, pady=4)
        self.role_permission_role = ttk.Combobox(role_perm_frame, values=self._role_names(), width=25, state="readonly")
        self.role_permission_role.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(role_perm_frame, text="ID прав (через запятую)").grid(row=0, column=2, padx=4, pady=4)
        self.role_permission_ids = ttk.Entry(role_perm_frame, width=30)
        self.role_permission_ids.grid(row=0, column=3, padx=4, pady=4)
        ttk.Button(role_perm_frame, text="Назначить права", command=self._assign_role_permissions).grid(
            row=0, column=4, padx=4, pady=4
        )

        users_frame = ttk.LabelFrame(frame, text="Пользователи")
        users_frame.pack(fill=tk.BOTH, expand=True, padx=16, pady=8)
        user_form = ttk.Frame(users_frame)
        user_form.pack(fill=tk.X, padx=8, pady=4)
        ttk.Label(user_form, text="Логин").grid(row=0, column=0, padx=4, pady=4)
        self.user_username = ttk.Entry(user_form, width=20)
        self.user_username.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(user_form, text="ФИО").grid(row=0, column=2, padx=4, pady=4)
        self.user_fullname = ttk.Entry(user_form, width=30)
        self.user_fullname.grid(row=0, column=3, padx=4, pady=4)
        self.user_active = tk.BooleanVar(value=True)
        ttk.Checkbutton(user_form, text="Активен", variable=self.user_active).grid(row=0, column=4, padx=4, pady=4)
        ttk.Button(user_form, text="Добавить", command=self._add_user).grid(row=0, column=5, padx=4, pady=4)
        ttk.Button(user_form, text="Обновить", command=self._refresh_users).grid(row=0, column=6, padx=4, pady=4)
        self.users_tree = self._make_tree(users_frame, ["ID", "Логин", "ФИО", "Активен"])
        self.users_tree.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)

        user_role_frame = ttk.Frame(users_frame)
        user_role_frame.pack(fill=tk.X, padx=8, pady=4)
        ttk.Label(user_role_frame, text="Пользователь").grid(row=0, column=0, padx=4, pady=4)
        self.user_role_user = ttk.Combobox(user_role_frame, values=self._user_names(), width=25, state="readonly")
        self.user_role_user.grid(row=0, column=1, padx=4, pady=4)
        ttk.Label(user_role_frame, text="ID ролей (через запятую)").grid(row=0, column=2, padx=4, pady=4)
        self.user_role_ids = ttk.Entry(user_role_frame, width=30)
        self.user_role_ids.grid(row=0, column=3, padx=4, pady=4)
        ttk.Button(user_role_frame, text="Назначить роли", command=self._assign_user_roles).grid(
            row=0, column=4, padx=4, pady=4
        )

        self._refresh_roles()
        self._refresh_permissions()
        self._refresh_users()

    def _make_tree(self, parent: tk.Widget, columns: list[str]) -> ttk.Treeview:
        tree = ttk.Treeview(parent, columns=columns, show="headings")
        for col in columns:
            tree.heading(col, text=col)
            tree.column(col, width=140, anchor="w")
        return tree

    def _clear_tree(self, tree: ttk.Treeview) -> None:
        for item in tree.get_children():
            tree.delete(item)

    def _client_names(self) -> list[str]:
        return [f"{client.id} - {client.name}" for client in list_clients(self.conn)]

    def _warehouse_names(self) -> list[str]:
        return [f"{warehouse.id} - {warehouse.name}" for warehouse in list_warehouses(self.conn)]

    def _product_names(self) -> list[str]:
        return [f"{product.id} - {product.sku} ({product.name})" for product in list_products(self.conn)]

    def _location_names(self) -> list[str]:
        return [
            f"{row['id']} - {row['warehouse']} {row['zone']}/{row['code']}"
            for row in list_locations(self.conn)
        ]

    def _role_names(self) -> list[str]:
        return [f"{row['id']} - {row['name']}" for row in list_roles(self.conn)]

    def _user_names(self) -> list[str]:
        return [f"{row['id']} - {row['username']}" for row in list_users(self.conn)]

    def _refresh_dashboard(self) -> None:
        clients = list_clients(self.conn)
        warehouses = list_warehouses(self.conn)
        products = list_products(self.conn)
        self.dashboard_stats.config(
            text=f"Клиенты: {len(clients)} | Склады: {len(warehouses)} | Товары: {len(products)}"
        )

        self._clear_tree(self.dashboard_inventory)
        for row in inventory_report(self.conn):
            self.dashboard_inventory.insert(
                "", tk.END, values=(row["warehouse"], row["sku"], row["product"], row["unit"], row["quantity"])
            )

        self._clear_tree(self.dashboard_movements)
        for row in movement_report(self.conn, limit=10):
            self.dashboard_movements.insert(
                "",
                tk.END,
                values=(
                    row["created_at"],
                    row["warehouse"],
                    row["sku"],
                    row["product"],
                    row["quantity_change"],
                    row["reason"],
                ),
            )

    def _refresh_clients(self) -> None:
        self._clear_tree(self.clients_tree)
        for client in list_clients(self.conn):
            self.clients_tree.insert("", tk.END, values=(client.id, client.name))
        self._refresh_dashboard()

    def _add_client(self) -> None:
        name = self.client_name.get().strip()
        if not name:
            messagebox.showwarning("Ошибка", "Введите название клиента.")
            return
        try:
            add_client(self.conn, name)
            self.client_name.delete(0, tk.END)
            self._refresh_clients()
            self._refresh_comboboxes()
        except sqlite3.IntegrityError as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _edit_client(self) -> None:
        selected = self.clients_tree.selection()
        if not selected:
            messagebox.showwarning("Ошибка", "Выберите клиента для редактирования.")
            return
        item = self.clients_tree.item(selected[0])
        client_id, name = item["values"]
        new_name = self._prompt("Изменить клиента", "Новое название:", name)
        if new_name:
            self.conn.execute("UPDATE clients SET name = ? WHERE id = ?", (new_name, client_id))
            self.conn.commit()
            self._refresh_clients()
            self._refresh_comboboxes()

    def _delete_client(self) -> None:
        selected = self.clients_tree.selection()
        if not selected:
            messagebox.showwarning("Ошибка", "Выберите клиента для удаления.")
            return
        item = self.clients_tree.item(selected[0])
        client_id = item["values"][0]
        if messagebox.askyesno("Подтверждение", "Удалить клиента?"):
            self.conn.execute("DELETE FROM clients WHERE id = ?", (client_id,))
            self.conn.commit()
            self._refresh_clients()
            self._refresh_comboboxes()

    def _refresh_warehouses(self) -> None:
        self._clear_tree(self.warehouses_tree)
        for warehouse in list_warehouses(self.conn):
            self.warehouses_tree.insert("", tk.END, values=(warehouse.id, warehouse.name, warehouse.location))
        self._refresh_dashboard()

    def _add_warehouse(self) -> None:
        name = self.warehouse_name.get().strip()
        location = self.warehouse_location.get().strip()
        if not name or not location:
            messagebox.showwarning("Ошибка", "Введите название и локацию склада.")
            return
        try:
            add_warehouse(self.conn, name, location)
            self.warehouse_name.delete(0, tk.END)
            self.warehouse_location.delete(0, tk.END)
            self._refresh_warehouses()
            self._refresh_comboboxes()
        except sqlite3.IntegrityError as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _edit_warehouse(self) -> None:
        selected = self.warehouses_tree.selection()
        if not selected:
            messagebox.showwarning("Ошибка", "Выберите склад для редактирования.")
            return
        item = self.warehouses_tree.item(selected[0])
        warehouse_id, name, location = item["values"]
        new_name = self._prompt("Склад", "Новое название:", name)
        if new_name is None:
            return
        new_location = self._prompt("Склад", "Новая локация:", location)
        if new_location is None:
            return
        self.conn.execute(
            "UPDATE warehouses SET name = ?, location = ? WHERE id = ?",
            (new_name, new_location, warehouse_id),
        )
        self.conn.commit()
        self._refresh_warehouses()
        self._refresh_comboboxes()

    def _delete_warehouse(self) -> None:
        selected = self.warehouses_tree.selection()
        if not selected:
            messagebox.showwarning("Ошибка", "Выберите склад для удаления.")
            return
        warehouse_id = self.warehouses_tree.item(selected[0])["values"][0]
        if messagebox.askyesno("Подтверждение", "Удалить склад?"):
            self.conn.execute("DELETE FROM warehouses WHERE id = ?", (warehouse_id,))
            self.conn.commit()
            self._refresh_warehouses()
            self._refresh_comboboxes()

    def _refresh_locations(self) -> None:
        self._clear_tree(self.locations_tree)
        for row in list_locations(self.conn):
            self.locations_tree.insert(
                "",
                tk.END,
                values=(row["id"], row["warehouse"], row["zone"], row["code"], row["description"] or ""),
            )
        self._refresh_dashboard()

    def _add_location(self) -> None:
        warehouse = self.location_warehouse.get()
        zone = self.location_zone.get().strip()
        code = self.location_code.get().strip()
        description = self.location_description.get().strip()
        if not warehouse or not zone or not code:
            messagebox.showwarning("Ошибка", "Заполните склад, зону и код.")
            return
        warehouse_id = int(warehouse.split(" - ")[0])
        try:
            add_location(self.conn, warehouse_id, zone, code, description)
            self.location_zone.delete(0, tk.END)
            self.location_code.delete(0, tk.END)
            self.location_description.delete(0, tk.END)
            self._refresh_locations()
            self._refresh_comboboxes()
        except sqlite3.IntegrityError as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _edit_location(self) -> None:
        selected = self.locations_tree.selection()
        if not selected:
            messagebox.showwarning("Ошибка", "Выберите локацию для редактирования.")
            return
        item = self.locations_tree.item(selected[0])
        location_id, warehouse, zone, code, description = item["values"]
        new_zone = self._prompt("Локация", "Зона:", zone)
        if new_zone is None:
            return
        new_code = self._prompt("Локация", "Код:", code)
        if new_code is None:
            return
        new_desc = self._prompt("Локация", "Описание:", description)
        if new_desc is None:
            return
        update_location(self.conn, location_id, new_zone, new_code, new_desc)
        self._refresh_locations()
        self._refresh_comboboxes()

    def _delete_location(self) -> None:
        selected = self.locations_tree.selection()
        if not selected:
            messagebox.showwarning("Ошибка", "Выберите локацию для удаления.")
            return
        location_id = self.locations_tree.item(selected[0])["values"][0]
        if messagebox.askyesno("Подтверждение", "Удалить локацию?"):
            delete_location(self.conn, location_id)
            self._refresh_locations()
            self._refresh_comboboxes()

    def _refresh_products(self) -> None:
        self._clear_tree(self.products_tree)
        for product in list_products(self.conn):
            self.products_tree.insert("", tk.END, values=(product.id, product.sku, product.name, product.unit))
        self._refresh_dashboard()

    def _add_product(self) -> None:
        sku = self.product_sku.get().strip()
        name = self.product_name.get().strip()
        unit = self.product_unit.get().strip()
        if not sku or not name or not unit:
            messagebox.showwarning("Ошибка", "Заполните все поля товара.")
            return
        try:
            add_product(self.conn, sku, name, unit)
            self.product_sku.delete(0, tk.END)
            self.product_name.delete(0, tk.END)
            self.product_unit.delete(0, tk.END)
            self._refresh_products()
            self._refresh_comboboxes()
        except sqlite3.IntegrityError as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _edit_product(self) -> None:
        selected = self.products_tree.selection()
        if not selected:
            messagebox.showwarning("Ошибка", "Выберите товар для редактирования.")
            return
        item = self.products_tree.item(selected[0])
        product_id, sku, name, unit = item["values"]
        new_sku = self._prompt("Товар", "SKU:", sku)
        if new_sku is None:
            return
        new_name = self._prompt("Товар", "Название:", name)
        if new_name is None:
            return
        new_unit = self._prompt("Товар", "Ед.:", unit)
        if new_unit is None:
            return
        self.conn.execute(
            "UPDATE products SET sku = ?, name = ?, unit = ? WHERE id = ?",
            (new_sku, new_name, new_unit, product_id),
        )
        self.conn.commit()
        self._refresh_products()
        self._refresh_comboboxes()

    def _delete_product(self) -> None:
        selected = self.products_tree.selection()
        if not selected:
            messagebox.showwarning("Ошибка", "Выберите товар для удаления.")
            return
        product_id = self.products_tree.item(selected[0])["values"][0]
        if messagebox.askyesno("Подтверждение", "Удалить товар?"):
            self.conn.execute("DELETE FROM products WHERE id = ?", (product_id,))
            self.conn.commit()
            self._refresh_products()
            self._refresh_comboboxes()

    def _refresh_inbound_orders(self) -> None:
        self._clear_tree(self.inbound_orders_tree)
        rows = fetch_all(
            self.conn,
            "SELECT o.id, c.name AS client, w.name AS warehouse, o.status, o.created_at "
            "FROM inbound_orders o "
            "JOIN clients c ON c.id = o.client_id "
            "JOIN warehouses w ON w.id = o.warehouse_id "
            "ORDER BY o.created_at DESC",
        )
        for row in rows:
            self.inbound_orders_tree.insert(
                "", tk.END, values=(row["id"], row["client"], row["warehouse"], row["status"], row["created_at"])
            )
        self._refresh_dashboard()

    def _create_inbound(self) -> None:
        client = self.inbound_client.get()
        warehouse = self.inbound_warehouse.get()
        if not client or not warehouse:
            messagebox.showwarning("Ошибка", "Выберите клиента и склад.")
            return
        client_id = int(client.split(" - ")[0])
        warehouse_id = int(warehouse.split(" - ")[0])
        create_inbound_order(self.conn, client_id, warehouse_id)
        self._refresh_inbound_orders()

    def _add_inbound_item(self) -> None:
        order_id = self.inbound_order_id.get().strip()
        product = self.inbound_product.get()
        quantity = self.inbound_quantity.get().strip()
        location = self.inbound_location.get()
        if not order_id or not product or not quantity or not location:
            messagebox.showwarning("Ошибка", "Заполните все поля позиции.")
            return
        product_id = int(product.split(" - ")[0])
        location_id = int(location.split(" - ")[0])
        add_inbound_item(
            self.conn,
            int(order_id),
            product_id,
            float(quantity),
            lot=self.inbound_lot.get().strip() or None,
            serial=self.inbound_serial.get().strip() or None,
            expiry=self.inbound_expiry.get().strip() or None,
            location_id=location_id,
        )
        self._refresh_inbound_orders()

    def _receive_inbound(self) -> None:
        order_id = self.inbound_order_id.get().strip()
        if not order_id:
            messagebox.showwarning("Ошибка", "Укажите ID прихода.")
            return
        receive_inbound_order(self.conn, int(order_id))
        self._refresh_inbound_orders()
        self._refresh_inventory()
        self._refresh_movements()

    def _refresh_outbound_orders(self) -> None:
        self._clear_tree(self.outbound_orders_tree)
        rows = fetch_all(
            self.conn,
            "SELECT o.id, c.name AS client, w.name AS warehouse, o.status, o.created_at "
            "FROM outbound_orders o "
            "JOIN clients c ON c.id = o.client_id "
            "JOIN warehouses w ON w.id = o.warehouse_id "
            "ORDER BY o.created_at DESC",
        )
        for row in rows:
            self.outbound_orders_tree.insert(
                "", tk.END, values=(row["id"], row["client"], row["warehouse"], row["status"], row["created_at"])
            )
        self._refresh_dashboard()

    def _create_outbound(self) -> None:
        client = self.outbound_client.get()
        warehouse = self.outbound_warehouse.get()
        if not client or not warehouse:
            messagebox.showwarning("Ошибка", "Выберите клиента и склад.")
            return
        client_id = int(client.split(" - ")[0])
        warehouse_id = int(warehouse.split(" - ")[0])
        create_outbound_order(self.conn, client_id, warehouse_id)
        self._refresh_outbound_orders()

    def _add_outbound_item(self) -> None:
        order_id = self.outbound_order_id.get().strip()
        product = self.outbound_product.get()
        quantity = self.outbound_quantity.get().strip()
        location = self.outbound_location.get()
        if not order_id or not product or not quantity or not location:
            messagebox.showwarning("Ошибка", "Заполните все поля позиции.")
            return
        product_id = int(product.split(" - ")[0])
        location_id = int(location.split(" - ")[0])
        add_outbound_item(
            self.conn,
            int(order_id),
            product_id,
            float(quantity),
            lot=self.outbound_lot.get().strip() or None,
            serial=self.outbound_serial.get().strip() or None,
            expiry=self.outbound_expiry.get().strip() or None,
            location_id=location_id,
        )
        self._refresh_outbound_orders()

    def _ship_outbound(self) -> None:
        order_id = self.outbound_order_id.get().strip()
        if not order_id:
            messagebox.showwarning("Ошибка", "Укажите ID отгрузки.")
            return
        ship_outbound_order(self.conn, int(order_id))
        self._refresh_outbound_orders()
        self._refresh_inventory()
        self._refresh_movements()

    def _refresh_inventory(self) -> None:
        self._clear_tree(self.inventory_tree)
        selected = self.inventory_warehouse.get()
        warehouse_id = None
        if selected and selected != "Все":
            warehouse_id = int(selected.split(" - ")[0])
        for row in inventory_report(self.conn, warehouse_id):
            self.inventory_tree.insert(
                "", tk.END, values=(row["warehouse"], row["sku"], row["product"], row["unit"], row["quantity"])
            )

    def _refresh_movements(self) -> None:
        self._clear_tree(self.movements_tree)
        limit = int(self.movements_limit.get() or 50)
        for row in movement_report(self.conn, limit):
            self.movements_tree.insert(
                "",
                tk.END,
                values=(
                    row["created_at"],
                    row["warehouse"],
                    row["sku"],
                    row["product"],
                    row["quantity_change"],
                    row["reason"],
                    row["ref_type"],
                    row["ref_id"],
                ),
            )

    def _refresh_adjustments(self) -> None:
        self._clear_tree(self.adjustments_tree)
        rows = fetch_all(
            self.conn,
            "SELECT a.id, l.zone, l.code, p.sku, p.name AS product, a.lot, a.serial, a.expiry, "
            "a.quantity_change, a.reason, a.created_at "
            "FROM inventory_adjustments a "
            "JOIN locations l ON l.id = a.location_id "
            "JOIN products p ON p.id = a.product_id "
            "ORDER BY a.created_at DESC",
        )
        for row in rows:
            self.adjustments_tree.insert(
                "",
                tk.END,
                values=(
                    row["id"],
                    f"{row['zone']}/{row['code']}",
                    row["sku"],
                    row["product"],
                    row["lot"],
                    row["serial"],
                    row["expiry"],
                    row["quantity_change"],
                    row["reason"],
                    row["created_at"],
                ),
            )

    def _apply_adjustment(self) -> None:
        location = self.adjust_location.get()
        product = self.adjust_product.get()
        quantity = self.adjust_quantity.get().strip()
        reason = self.adjust_reason.get().strip()
        if not location or not product or not quantity or not reason:
            messagebox.showwarning("Ошибка", "Заполните все поля инвентаризации.")
            return
        location_id = int(location.split(" - ")[0])
        product_id = int(product.split(" - ")[0])
        qty = float(quantity)
        add_inventory_adjustment(
            self.conn,
            location_id,
            product_id,
            self.adjust_lot.get().strip() or None,
            self.adjust_serial.get().strip() or None,
            self.adjust_expiry.get().strip() or None,
            qty,
            reason,
        )
        location_row = self.conn.execute(
            "SELECT warehouse_id FROM locations WHERE id = ?",
            (location_id,),
        ).fetchone()
        if location_row:
            adjust_stock_lot(
                self.conn,
                location_row["warehouse_id"],
                location_id,
                product_id,
                self.adjust_lot.get().strip() or None,
                self.adjust_serial.get().strip() or None,
                self.adjust_expiry.get().strip() or None,
                qty,
            )
            ensure_inventory_row(self.conn, location_row["warehouse_id"], product_id)
            self.conn.execute(
                "UPDATE inventory SET quantity = quantity + ? WHERE warehouse_id = ? AND product_id = ?",
                (qty, location_row["warehouse_id"], product_id),
            )
            self.conn.commit()
        self._refresh_adjustments()
        self._refresh_inventory()
        self._refresh_movements()

    def _refresh_moves(self) -> None:
        self._clear_tree(self.moves_tree)
        rows = fetch_all(
            self.conn,
            "SELECT m.id, lf.zone AS from_zone, lf.code AS from_code, "
            "lt.zone AS to_zone, lt.code AS to_code, p.sku, p.name AS product, "
            "m.lot, m.serial, m.expiry, m.quantity, m.reason, m.created_at "
            "FROM storage_moves m "
            "JOIN locations lf ON lf.id = m.from_location_id "
            "JOIN locations lt ON lt.id = m.to_location_id "
            "JOIN products p ON p.id = m.product_id "
            "ORDER BY m.created_at DESC",
        )
        for row in rows:
            self.moves_tree.insert(
                "",
                tk.END,
                values=(
                    row["id"],
                    f"{row['from_zone']}/{row['from_code']}",
                    f"{row['to_zone']}/{row['to_code']}",
                    row["sku"],
                    row["product"],
                    row["lot"],
                    row["serial"],
                    row["expiry"],
                    row["quantity"],
                    row["reason"],
                    row["created_at"],
                ),
            )

    def _apply_move(self) -> None:
        from_loc = self.move_from.get()
        to_loc = self.move_to.get()
        product = self.move_product.get()
        quantity = self.move_quantity.get().strip()
        reason = self.move_reason.get().strip()
        if not from_loc or not to_loc or not product or not quantity or not reason:
            messagebox.showwarning("Ошибка", "Заполните все поля перемещения.")
            return
        from_id = int(from_loc.split(" - ")[0])
        to_id = int(to_loc.split(" - ")[0])
        product_id = int(product.split(" - ")[0])
        qty = float(quantity)
        add_storage_move(
            self.conn,
            from_id,
            to_id,
            product_id,
            self.move_lot.get().strip() or None,
            self.move_serial.get().strip() or None,
            self.move_expiry.get().strip() or None,
            qty,
            reason,
        )
        from_wh = self.conn.execute("SELECT warehouse_id FROM locations WHERE id = ?", (from_id,)).fetchone()
        to_wh = self.conn.execute("SELECT warehouse_id FROM locations WHERE id = ?", (to_id,)).fetchone()
        if from_wh and to_wh:
            adjust_stock_lot(
                self.conn,
                from_wh["warehouse_id"],
                from_id,
                product_id,
                self.move_lot.get().strip() or None,
                self.move_serial.get().strip() or None,
                self.move_expiry.get().strip() or None,
                -qty,
            )
            adjust_stock_lot(
                self.conn,
                to_wh["warehouse_id"],
                to_id,
                product_id,
                self.move_lot.get().strip() or None,
                self.move_serial.get().strip() or None,
                self.move_expiry.get().strip() or None,
                qty,
            )
        self._refresh_moves()
        self._refresh_inventory()
        self._refresh_movements()

    def _refresh_reports(self) -> None:
        rows = list_stock_lots(self.conn)
        self._clear_tree(self.reports_inventory_tree)
        for row in rows:
            self.reports_inventory_tree.insert(
                "",
                tk.END,
                values=(
                    row["warehouse"],
                    f"{row['zone']}/{row['code']}",
                    row["sku"],
                    row["product"],
                    row["lot"],
                    row["serial"],
                    row["expiry"],
                    row["quantity"],
                ),
            )
        self._clear_tree(self.reports_movement_tree)
        for row in movement_report(self.conn, limit=200):
            self.reports_movement_tree.insert(
                "",
                tk.END,
                values=(
                    row["created_at"],
                    row["warehouse"],
                    row["sku"],
                    row["product"],
                    row["quantity_change"],
                    row["reason"],
                    row["ref_type"],
                    row["ref_id"],
                ),
            )
        self.reports_summary.config(text=f"Записей по лотам: {len(rows)} | Движений: 200")

    def _refresh_finance(self) -> None:
        self._clear_tree(self.billing_rates_tree)
        for row in list_billing_rates(self.conn):
            self.billing_rates_tree.insert(
                "",
                tk.END,
                values=(
                    row["id"],
                    row["client"],
                    row["storage_rate"],
                    row["handling_rate"],
                    row["currency"],
                    row["effective_from"],
                ),
            )
        self._clear_tree(self.billing_records_tree)
        for row in list_billing_records(self.conn):
            self.billing_records_tree.insert(
                "",
                tk.END,
                values=(
                    row["id"],
                    row["client"],
                    row["service_type"],
                    row["quantity"],
                    row["unit_price"],
                    row["amount"],
                    row["period"],
                    row["created_at"],
                ),
            )

    def _add_billing_rate(self) -> None:
        client = self.billing_client.get()
        if not client:
            messagebox.showwarning("Ошибка", "Выберите клиента.")
            return
        try:
            add_billing_rate(
                self.conn,
                int(client.split(" - ")[0]),
                float(self.billing_storage_rate.get()),
                float(self.billing_handling_rate.get()),
                self.billing_currency.get().strip(),
                self.billing_effective.get().strip(),
            )
            self._refresh_finance()
        except (ValueError, sqlite3.IntegrityError) as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _add_billing_record(self) -> None:
        client = self.charge_client.get()
        if not client:
            messagebox.showwarning("Ошибка", "Выберите клиента.")
            return
        try:
            add_billing_record(
                self.conn,
                int(client.split(" - ")[0]),
                self.charge_service.get().strip(),
                float(self.charge_quantity.get()),
                float(self.charge_unit_price.get()),
                self.charge_period.get().strip(),
            )
            self._refresh_finance()
        except (ValueError, sqlite3.IntegrityError) as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _refresh_roles(self) -> None:
        self._clear_tree(self.roles_tree)
        for row in list_roles(self.conn):
            self.roles_tree.insert("", tk.END, values=(row["id"], row["name"], row["description"]))
        self.role_permission_role["values"] = self._role_names()

    def _add_role(self) -> None:
        name = self.role_name.get().strip()
        if not name:
            messagebox.showwarning("Ошибка", "Введите название роли.")
            return
        try:
            add_role(self.conn, name, self.role_description.get().strip())
            self.role_name.delete(0, tk.END)
            self.role_description.delete(0, tk.END)
            self._refresh_roles()
        except sqlite3.IntegrityError as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _refresh_permissions(self) -> None:
        self._clear_tree(self.permissions_tree)
        for row in list_permissions(self.conn):
            self.permissions_tree.insert("", tk.END, values=(row["id"], row["code"], row["description"]))

    def _add_permission(self) -> None:
        code = self.permission_code.get().strip()
        if not code:
            messagebox.showwarning("Ошибка", "Введите код права.")
            return
        try:
            add_permission(self.conn, code, self.permission_description.get().strip())
            self.permission_code.delete(0, tk.END)
            self.permission_description.delete(0, tk.END)
            self._refresh_permissions()
        except sqlite3.IntegrityError as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _assign_role_permissions(self) -> None:
        role = self.role_permission_role.get()
        if not role:
            messagebox.showwarning("Ошибка", "Выберите роль.")
            return
        try:
            role_id = int(role.split(" - ")[0])
            ids = [
                int(val.strip())
                for val in self.role_permission_ids.get().split(",")
                if val.strip()
            ]
            set_role_permissions(self.conn, role_id, ids)
            messagebox.showinfo("Готово", "Права назначены.")
        except ValueError as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _refresh_users(self) -> None:
        self._clear_tree(self.users_tree)
        for row in list_users(self.conn):
            self.users_tree.insert(
                "", tk.END, values=(row["id"], row["username"], row["full_name"], "Да" if row["is_active"] else "Нет")
            )
        self.user_role_user["values"] = self._user_names()

    def _add_user(self) -> None:
        username = self.user_username.get().strip()
        full_name = self.user_fullname.get().strip()
        if not username or not full_name:
            messagebox.showwarning("Ошибка", "Введите логин и ФИО.")
            return
        try:
            add_user(self.conn, username, full_name, self.user_active.get())
            self.user_username.delete(0, tk.END)
            self.user_fullname.delete(0, tk.END)
            self._refresh_users()
        except sqlite3.IntegrityError as exc:
            messagebox.showerror("Ошибка", str(exc))

    def _assign_user_roles(self) -> None:
        user = self.user_role_user.get()
        if not user:
            messagebox.showwarning("Ошибка", "Выберите пользователя.")
            return
        try:
            user_id = int(user.split(" - ")[0])
            ids = [
                int(val.strip())
                for val in self.user_role_ids.get().split(",")
                if val.strip()
            ]
            set_user_roles(self.conn, user_id, ids)
            messagebox.showinfo("Готово", "Роли назначены.")
        except ValueError as exc:
            messagebox.showerror("Ошибка", str(exc))
    def _refresh_comboboxes(self) -> None:
        self.inbound_client["values"] = self._client_names()
        self.outbound_client["values"] = self._client_names()
        self.inbound_warehouse["values"] = self._warehouse_names()
        self.outbound_warehouse["values"] = self._warehouse_names()
        self.inbound_product["values"] = self._product_names()
        self.outbound_product["values"] = self._product_names()
        self.inventory_warehouse["values"] = ["Все"] + self._warehouse_names()
        self.inbound_location["values"] = self._location_names()
        self.outbound_location["values"] = self._location_names()
        self.adjust_location["values"] = self._location_names()
        self.adjust_product["values"] = self._product_names()
        self.move_from["values"] = self._location_names()
        self.move_to["values"] = self._location_names()
        self.move_product["values"] = self._product_names()
        self.billing_client["values"] = self._client_names()
        self.charge_client["values"] = self._client_names()
        self.role_permission_role["values"] = self._role_names()
        self.user_role_user["values"] = self._user_names()

    def _prompt(self, title: str, label: str, initial: str) -> Optional[str]:
        dialog = tk.Toplevel(self)
        dialog.title(title)
        dialog.grab_set()
        ttk.Label(dialog, text=label).pack(padx=12, pady=8)
        entry = ttk.Entry(dialog, width=40)
        entry.insert(0, initial)
        entry.pack(padx=12, pady=8)
        result: list[Optional[str]] = [None]

        def submit() -> None:
            result[0] = entry.get().strip()
            dialog.destroy()

        ttk.Button(dialog, text="Сохранить", command=submit).pack(pady=8)
        dialog.wait_window()
        return result[0]


def main() -> None:
    app = WarehouseApp()
    app.mainloop()


if __name__ == "__main__":
    main()
