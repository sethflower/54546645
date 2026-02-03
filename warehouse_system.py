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

import argparse
import datetime as dt
import sqlite3
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


def print_table(rows: Iterable[sqlite3.Row], headers: list[str]) -> None:
    rows_list = list(rows)
    widths = [len(header) for header in headers]
    for row in rows_list:
        for idx, header in enumerate(headers):
            widths[idx] = max(widths[idx], len(str(row[header])))
    line = " | ".join(header.ljust(widths[idx]) for idx, header in enumerate(headers))
    print(line)
    print("-+-".join("-" * width for width in widths))
    for row in rows_list:
        print(" | ".join(str(row[header]).ljust(widths[idx]) for idx, header in enumerate(headers)))


def run_interactive(conn: sqlite3.Connection) -> None:
    menu = {
        "1": "Add client",
        "2": "Add warehouse",
        "3": "Add product",
        "4": "Create inbound order",
        "5": "Add inbound item",
        "6": "Receive inbound order",
        "7": "Create outbound order",
        "8": "Add outbound item",
        "9": "Ship outbound order",
        "10": "Inventory report",
        "11": "Movement report",
        "12": "List master data",
        "0": "Exit",
    }
    while True:
        print("\n=== 3PL Warehouse Management ===")
        for key, label in menu.items():
            print(f"{key}. {label}")
        choice = input("Select option: ").strip()

        try:
            if choice == "1":
                name = input("Client name: ").strip()
                client_id = add_client(conn, name)
                print(f"Client created with ID {client_id}")
            elif choice == "2":
                name = input("Warehouse name: ").strip()
                location = input("Location: ").strip()
                warehouse_id = add_warehouse(conn, name, location)
                print(f"Warehouse created with ID {warehouse_id}")
            elif choice == "3":
                sku = input("SKU: ").strip()
                name = input("Product name: ").strip()
                unit = input("Unit (e.g. pcs, kg): ").strip()
                product_id = add_product(conn, sku, name, unit)
                print(f"Product created with ID {product_id}")
            elif choice == "4":
                client_id = int(input("Client ID: ").strip())
                warehouse_id = int(input("Warehouse ID: ").strip())
                order_id = create_inbound_order(conn, client_id, warehouse_id)
                print(f"Inbound order created with ID {order_id}")
            elif choice == "5":
                order_id = int(input("Inbound order ID: ").strip())
                product_id = int(input("Product ID: ").strip())
                quantity = float(input("Quantity: ").strip())
                add_inbound_item(conn, order_id, product_id, quantity)
                print("Inbound item added")
            elif choice == "6":
                order_id = int(input("Inbound order ID: ").strip())
                receive_inbound_order(conn, order_id)
                print("Inbound order received")
            elif choice == "7":
                client_id = int(input("Client ID: ").strip())
                warehouse_id = int(input("Warehouse ID: ").strip())
                order_id = create_outbound_order(conn, client_id, warehouse_id)
                print(f"Outbound order created with ID {order_id}")
            elif choice == "8":
                order_id = int(input("Outbound order ID: ").strip())
                product_id = int(input("Product ID: ").strip())
                quantity = float(input("Quantity: ").strip())
                add_outbound_item(conn, order_id, product_id, quantity)
                print("Outbound item added")
            elif choice == "9":
                order_id = int(input("Outbound order ID: ").strip())
                ship_outbound_order(conn, order_id)
                print("Outbound order shipped")
            elif choice == "10":
                warehouse_raw = input("Warehouse ID (blank for all): ").strip()
                warehouse_id = int(warehouse_raw) if warehouse_raw else None
                rows = inventory_report(conn, warehouse_id)
                if rows:
                    print_table(rows, ["warehouse", "sku", "product", "unit", "quantity"])
                else:
                    print("No inventory records")
            elif choice == "11":
                limit_raw = input("Limit (default 50): ").strip()
                limit = int(limit_raw) if limit_raw else 50
                rows = movement_report(conn, limit)
                if rows:
                    print_table(
                        rows,
                        [
                            "created_at",
                            "warehouse",
                            "sku",
                            "product",
                            "quantity_change",
                            "reason",
                            "ref_type",
                            "ref_id",
                        ],
                    )
                else:
                    print("No movements found")
            elif choice == "12":
                print("\nClients:")
                for client in list_clients(conn):
                    print(f"{client.id}: {client.name}")
                print("\nWarehouses:")
                for warehouse in list_warehouses(conn):
                    print(f"{warehouse.id}: {warehouse.name} ({warehouse.location})")
                print("\nProducts:")
                for product in list_products(conn):
                    print(f"{product.id}: {product.sku} - {product.name} ({product.unit})")
            elif choice == "0":
                print("Goodbye")
                break
            else:
                print("Unknown option")
        except (ValueError, sqlite3.IntegrityError) as exc:
            print(f"Error: {exc}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="3PL warehouse management system")
    parser.add_argument("--db", default=DB_PATH_DEFAULT, help="Path to SQLite database")

    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("init", help="Initialize database")

    add_client_parser = subparsers.add_parser("add-client", help="Add client")
    add_client_parser.add_argument("name")

    add_warehouse_parser = subparsers.add_parser("add-warehouse", help="Add warehouse")
    add_warehouse_parser.add_argument("name")
    add_warehouse_parser.add_argument("location")

    add_product_parser = subparsers.add_parser("add-product", help="Add product")
    add_product_parser.add_argument("sku")
    add_product_parser.add_argument("name")
    add_product_parser.add_argument("unit")

    inbound_parser = subparsers.add_parser("create-inbound", help="Create inbound order")
    inbound_parser.add_argument("client_id", type=int)
    inbound_parser.add_argument("warehouse_id", type=int)

    inbound_item_parser = subparsers.add_parser("add-inbound-item", help="Add inbound order item")
    inbound_item_parser.add_argument("order_id", type=int)
    inbound_item_parser.add_argument("product_id", type=int)
    inbound_item_parser.add_argument("quantity", type=float)

    receive_inbound_parser = subparsers.add_parser("receive-inbound", help="Receive inbound order")
    receive_inbound_parser.add_argument("order_id", type=int)

    outbound_parser = subparsers.add_parser("create-outbound", help="Create outbound order")
    outbound_parser.add_argument("client_id", type=int)
    outbound_parser.add_argument("warehouse_id", type=int)

    outbound_item_parser = subparsers.add_parser("add-outbound-item", help="Add outbound order item")
    outbound_item_parser.add_argument("order_id", type=int)
    outbound_item_parser.add_argument("product_id", type=int)
    outbound_item_parser.add_argument("quantity", type=float)

    ship_outbound_parser = subparsers.add_parser("ship-outbound", help="Ship outbound order")
    ship_outbound_parser.add_argument("order_id", type=int)

    report_inventory_parser = subparsers.add_parser("report-inventory", help="Inventory report")
    report_inventory_parser.add_argument("--warehouse-id", type=int)

    report_movement_parser = subparsers.add_parser("report-movements", help="Movement report")
    report_movement_parser.add_argument("--limit", type=int, default=50)

    subparsers.add_parser("interactive", help="Run interactive menu")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    conn = connect(args.db)
    init_db(conn)

    if args.command in (None, "interactive"):
        run_interactive(conn)
        return

    try:
        if args.command == "init":
            print("Database initialized")
        elif args.command == "add-client":
            client_id = add_client(conn, args.name)
            print(f"Client created with ID {client_id}")
        elif args.command == "add-warehouse":
            warehouse_id = add_warehouse(conn, args.name, args.location)
            print(f"Warehouse created with ID {warehouse_id}")
        elif args.command == "add-product":
            product_id = add_product(conn, args.sku, args.name, args.unit)
            print(f"Product created with ID {product_id}")
        elif args.command == "create-inbound":
            order_id = create_inbound_order(conn, args.client_id, args.warehouse_id)
            print(f"Inbound order created with ID {order_id}")
        elif args.command == "add-inbound-item":
            add_inbound_item(conn, args.order_id, args.product_id, args.quantity)
            print("Inbound item added")
        elif args.command == "receive-inbound":
            receive_inbound_order(conn, args.order_id)
            print("Inbound order received")
        elif args.command == "create-outbound":
            order_id = create_outbound_order(conn, args.client_id, args.warehouse_id)
            print(f"Outbound order created with ID {order_id}")
        elif args.command == "add-outbound-item":
            add_outbound_item(conn, args.order_id, args.product_id, args.quantity)
            print("Outbound item added")
        elif args.command == "ship-outbound":
            ship_outbound_order(conn, args.order_id)
            print("Outbound order shipped")
        elif args.command == "report-inventory":
            rows = inventory_report(conn, args.warehouse_id)
            if rows:
                print_table(rows, ["warehouse", "sku", "product", "unit", "quantity"])
            else:
                print("No inventory records")
        elif args.command == "report-movements":
            rows = movement_report(conn, args.limit)
            if rows:
                print_table(
                    rows,
                    [
                        "created_at",
                        "warehouse",
                        "sku",
                        "product",
                        "quantity_change",
                        "reason",
                        "ref_type",
                        "ref_id",
                    ],
                )
            else:
                print("No movements found")
        else:
            parser.print_help()
    except (ValueError, sqlite3.IntegrityError) as exc:
        print(f"Error: {exc}")


if __name__ == "__main__":
    main()
