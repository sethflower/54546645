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
    conn: sqlite3.Connection, inbound_order_id: int, product_id: int, quantity: float
) -> int:
    cursor = conn.execute(
        "INSERT INTO inbound_items (inbound_order_id, product_id, quantity) VALUES (?, ?, ?)",
        (inbound_order_id, product_id, quantity),
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
        "SELECT product_id, quantity FROM inbound_items WHERE inbound_order_id = ?",
        (inbound_order_id,),
    ).fetchall()
    if not items:
        raise ValueError("Inbound order has no items")

    for item in items:
        ensure_inventory_row(conn, order["warehouse_id"], item["product_id"])
        conn.execute(
            "UPDATE inventory SET quantity = quantity + ? WHERE warehouse_id = ? AND product_id = ?",
            (item["quantity"], order["warehouse_id"], item["product_id"]),
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
    conn: sqlite3.Connection, outbound_order_id: int, product_id: int, quantity: float
) -> int:
    cursor = conn.execute(
        "INSERT INTO outbound_items (outbound_order_id, product_id, quantity) VALUES (?, ?, ?)",
        (outbound_order_id, product_id, quantity),
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
        "SELECT product_id, quantity FROM outbound_items WHERE outbound_order_id = ?",
        (outbound_order_id,),
    ).fetchall()
    if not items:
        raise ValueError("Outbound order has no items")

    for item in items:
        row = conn.execute(
            "SELECT quantity FROM inventory WHERE warehouse_id = ? AND product_id = ?",
            (order["warehouse_id"], item["product_id"]),
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
        self._build_products_tab()
        self._build_inbound_tab()
        self._build_outbound_tab()
        self._build_inventory_tab()
        self._build_movements_tab()
        self._build_settings_tab()

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
        ttk.Label(item_form, text="Количество").grid(row=0, column=4, padx=4, pady=4)
        self.inbound_quantity = ttk.Entry(item_form, width=10)
        self.inbound_quantity.grid(row=0, column=5, padx=4, pady=4)
        ttk.Button(item_form, text="Добавить позицию", command=self._add_inbound_item).grid(row=0, column=6, padx=4, pady=4)
        ttk.Button(item_form, text="Принять", command=self._receive_inbound).grid(row=0, column=7, padx=4, pady=4)

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
        ttk.Label(item_form, text="Количество").grid(row=0, column=4, padx=4, pady=4)
        self.outbound_quantity = ttk.Entry(item_form, width=10)
        self.outbound_quantity.grid(row=0, column=5, padx=4, pady=4)
        ttk.Button(item_form, text="Добавить позицию", command=self._add_outbound_item).grid(row=0, column=6, padx=4, pady=4)
        ttk.Button(item_form, text="Отгрузить", command=self._ship_outbound).grid(row=0, column=7, padx=4, pady=4)

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

    def _build_settings_tab(self) -> None:
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="Администрирование")
        ttk.Label(
            frame,
            text=(
                "Админ-панель позволяет управлять справочниками и документами.\n"
                "Используйте вкладки выше для редактирования, удаления, настройки и контроля."
            ),
            justify=tk.LEFT,
        ).pack(anchor="w", padx=16, pady=16)

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
        if not order_id or not product or not quantity:
            messagebox.showwarning("Ошибка", "Заполните все поля позиции.")
            return
        product_id = int(product.split(" - ")[0])
        add_inbound_item(self.conn, int(order_id), product_id, float(quantity))
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
        if not order_id or not product or not quantity:
            messagebox.showwarning("Ошибка", "Заполните все поля позиции.")
            return
        product_id = int(product.split(" - ")[0])
        add_outbound_item(self.conn, int(order_id), product_id, float(quantity))
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

    def _refresh_comboboxes(self) -> None:
        self.inbound_client["values"] = self._client_names()
        self.outbound_client["values"] = self._client_names()
        self.inbound_warehouse["values"] = self._warehouse_names()
        self.outbound_warehouse["values"] = self._warehouse_names()
        self.inbound_product["values"] = self._product_names()
        self.outbound_product["values"] = self._product_names()
        self.inventory_warehouse["values"] = ["Все"] + self._warehouse_names()

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
