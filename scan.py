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
from dataclasses import dataclass
from typing import Iterable, Optional

from flask import Flask, redirect, render_template_string, request, url_for


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


TEMPLATES = {
    "base": """\
<!doctype html>
<html lang="ru">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>3PL Warehouse System</title>
    <style>
      :root {
        color-scheme: light;
        font-family: "Segoe UI", system-ui, sans-serif;
      }
      body {
        margin: 0;
        background: #f5f7fb;
        color: #1f2a44;
      }
      header {
        background: #1f4b99;
        color: #fff;
        padding: 16px 32px;
      }
      header h1 {
        margin: 0;
        font-size: 20px;
      }
      nav {
        background: #fff;
        padding: 12px 32px;
        border-bottom: 1px solid #e4e7ef;
        display: flex;
        gap: 16px;
        flex-wrap: wrap;
      }
      nav a {
        text-decoration: none;
        color: #1f4b99;
        font-weight: 600;
      }
      main {
        padding: 24px 32px 48px;
      }
      .grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
        gap: 16px;
      }
      .card {
        background: #fff;
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 2px 12px rgba(15, 23, 42, 0.08);
      }
      table {
        width: 100%;
        border-collapse: collapse;
        background: #fff;
      }
      th, td {
        text-align: left;
        padding: 8px 12px;
        border-bottom: 1px solid #e4e7ef;
      }
      th {
        background: #f0f4ff;
      }
      form.inline {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        align-items: flex-end;
      }
      input, select, button {
        padding: 8px 10px;
        border-radius: 8px;
        border: 1px solid #c9d3ea;
        font-size: 14px;
      }
      button {
        background: #1f4b99;
        color: #fff;
        border: none;
        cursor: pointer;
      }
      .muted {
        color: #6b7280;
        font-size: 14px;
      }
    </style>
  </head>
  <body>
    <header>
      <h1>3PL Warehouse Management System</h1>
    </header>
    <nav>
      <a href="{{ url_for('index') }}">Обзор</a>
      <a href="{{ url_for('clients_page') }}">Клиенты</a>
      <a href="{{ url_for('warehouses_page') }}">Склады</a>
      <a href="{{ url_for('products_page') }}">Товары</a>
      <a href="{{ url_for('inbound_new') }}">Новый приход</a>
      <a href="{{ url_for('outbound_new') }}">Новая отгрузка</a>
      <a href="{{ url_for('inventory_page') }}">Остатки</a>
      <a href="{{ url_for('movements_page') }}">Движения</a>
    </nav>
    <main>
      {% block content %}{% endblock %}
    </main>
  </body>
</html>
""",
    "index": """\
{% extends "base" %}
{% block content %}
  <div class="grid">
    <div class="card">
      <h3>Клиенты</h3>
      <p class="muted">Всего: {{ clients | length }}</p>
    </div>
    <div class="card">
      <h3>Склады</h3>
      <p class="muted">Всего: {{ warehouses | length }}</p>
    </div>
    <div class="card">
      <h3>Товары</h3>
      <p class="muted">Всего: {{ products | length }}</p>
    </div>
  </div>

  <h2>Остатки</h2>
  {% if inventory_rows %}
    <table>
      <thead>
        <tr>
          <th>Склад</th>
          <th>SKU</th>
          <th>Товар</th>
          <th>Ед.</th>
          <th>Кол-во</th>
        </tr>
      </thead>
      <tbody>
        {% for row in inventory_rows %}
          <tr>
            <td>{{ row['warehouse'] }}</td>
            <td>{{ row['sku'] }}</td>
            <td>{{ row['product'] }}</td>
            <td>{{ row['unit'] }}</td>
            <td>{{ row['quantity'] }}</td>
          </tr>
        {% endfor %}
      </tbody>
    </table>
  {% else %}
    <p class="muted">Нет остатков.</p>
  {% endif %}

  <h2>Последние движения</h2>
  {% if movement_rows %}
    <table>
      <thead>
        <tr>
          <th>Дата</th>
          <th>Склад</th>
          <th>SKU</th>
          <th>Товар</th>
          <th>Изменение</th>
          <th>Причина</th>
        </tr>
      </thead>
      <tbody>
        {% for row in movement_rows %}
          <tr>
            <td>{{ row['created_at'] }}</td>
            <td>{{ row['warehouse'] }}</td>
            <td>{{ row['sku'] }}</td>
            <td>{{ row['product'] }}</td>
            <td>{{ row['quantity_change'] }}</td>
            <td>{{ row['reason'] }}</td>
          </tr>
        {% endfor %}
      </tbody>
    </table>
  {% else %}
    <p class="muted">Нет движений.</p>
  {% endif %}
{% endblock %}
""",
    "clients": """\
{% extends "base" %}
{% block content %}
  <h2>Клиенты</h2>
  <form method="post" class="inline">
    <div>
      <label>Название</label><br />
      <input type="text" name="name" required />
    </div>
    <button type="submit">Добавить</button>
  </form>

  <table>
    <thead>
      <tr>
        <th>ID</th>
        <th>Название</th>
      </tr>
    </thead>
    <tbody>
      {% for client in clients %}
        <tr>
          <td>{{ client.id }}</td>
          <td>{{ client.name }}</td>
        </tr>
      {% else %}
        <tr>
          <td colspan="2" class="muted">Нет записей</td>
        </tr>
      {% endfor %}
    </tbody>
  </table>
{% endblock %}
""",
    "warehouses": """\
{% extends "base" %}
{% block content %}
  <h2>Склады</h2>
  <form method="post" class="inline">
    <div>
      <label>Название</label><br />
      <input type="text" name="name" required />
    </div>
    <div>
      <label>Локация</label><br />
      <input type="text" name="location" required />
    </div>
    <button type="submit">Добавить</button>
  </form>

  <table>
    <thead>
      <tr>
        <th>ID</th>
        <th>Название</th>
        <th>Локация</th>
      </tr>
    </thead>
    <tbody>
      {% for warehouse in warehouses %}
        <tr>
          <td>{{ warehouse.id }}</td>
          <td>{{ warehouse.name }}</td>
          <td>{{ warehouse.location }}</td>
        </tr>
      {% else %}
        <tr>
          <td colspan="3" class="muted">Нет записей</td>
        </tr>
      {% endfor %}
    </tbody>
  </table>
{% endblock %}
""",
    "products": """\
{% extends "base" %}
{% block content %}
  <h2>Товары</h2>
  <form method="post" class="inline">
    <div>
      <label>SKU</label><br />
      <input type="text" name="sku" required />
    </div>
    <div>
      <label>Название</label><br />
      <input type="text" name="name" required />
    </div>
    <div>
      <label>Ед.</label><br />
      <input type="text" name="unit" placeholder="pcs" required />
    </div>
    <button type="submit">Добавить</button>
  </form>

  <table>
    <thead>
      <tr>
        <th>ID</th>
        <th>SKU</th>
        <th>Название</th>
        <th>Ед.</th>
      </tr>
    </thead>
    <tbody>
      {% for product in products %}
        <tr>
          <td>{{ product.id }}</td>
          <td>{{ product.sku }}</td>
          <td>{{ product.name }}</td>
          <td>{{ product.unit }}</td>
        </tr>
      {% else %}
        <tr>
          <td colspan="4" class="muted">Нет записей</td>
        </tr>
      {% endfor %}
    </tbody>
  </table>
{% endblock %}
""",
    "inbound_new": """\
{% extends "base" %}
{% block content %}
  <h2>Новый приход</h2>
  <form method="post" class="inline">
    <div>
      <label>Клиент</label><br />
      <select name="client_id" required>
        {% for client in clients %}
          <option value="{{ client.id }}">{{ client.name }}</option>
        {% endfor %}
      </select>
    </div>
    <div>
      <label>Склад</label><br />
      <select name="warehouse_id" required>
        {% for warehouse in warehouses %}
          <option value="{{ warehouse.id }}">{{ warehouse.name }} ({{ warehouse.location }})</option>
        {% endfor %}
      </select>
    </div>
    <button type="submit">Создать приход</button>
  </form>
{% endblock %}
""",
    "inbound_detail": """\
{% extends "base" %}
{% block content %}
  <h2>Приход №{{ order.id }}</h2>
  <p class="muted">Клиент: {{ order.client }} · Склад: {{ order.warehouse }} · Статус: {{ order.status }}</p>

  <form method="post" class="inline">
    <input type="hidden" name="action" value="add-item" />
    <div>
      <label>Товар</label><br />
      <select name="product_id" required>
        {% for product in products %}
          <option value="{{ product.id }}">{{ product.sku }} — {{ product.name }}</option>
        {% endfor %}
      </select>
    </div>
    <div>
      <label>Количество</label><br />
      <input type="number" name="quantity" step="0.01" required />
    </div>
    <button type="submit">Добавить позицию</button>
  </form>

  <h3>Позиции</h3>
  <table>
    <thead>
      <tr>
        <th>SKU</th>
        <th>Товар</th>
        <th>Ед.</th>
        <th>Кол-во</th>
      </tr>
    </thead>
    <tbody>
      {% for item in items %}
        <tr>
          <td>{{ item.sku }}</td>
          <td>{{ item.name }}</td>
          <td>{{ item.unit }}</td>
          <td>{{ item.quantity }}</td>
        </tr>
      {% else %}
        <tr>
          <td colspan="4" class="muted">Нет позиций</td>
        </tr>
      {% endfor %}
    </tbody>
  </table>

  <form method="post" style="margin-top: 16px;">
    <input type="hidden" name="action" value="receive" />
    <button type="submit">Принять приход</button>
  </form>
{% endblock %}
""",
    "outbound_new": """\
{% extends "base" %}
{% block content %}
  <h2>Новая отгрузка</h2>
  <form method="post" class="inline">
    <div>
      <label>Клиент</label><br />
      <select name="client_id" required>
        {% for client in clients %}
          <option value="{{ client.id }}">{{ client.name }}</option>
        {% endfor %}
      </select>
    </div>
    <div>
      <label>Склад</label><br />
      <select name="warehouse_id" required>
        {% for warehouse in warehouses %}
          <option value="{{ warehouse.id }}">{{ warehouse.name }} ({{ warehouse.location }})</option>
        {% endfor %}
      </select>
    </div>
    <button type="submit">Создать отгрузку</button>
  </form>
{% endblock %}
""",
    "outbound_detail": """\
{% extends "base" %}
{% block content %}
  <h2>Отгрузка №{{ order.id }}</h2>
  <p class="muted">Клиент: {{ order.client }} · Склад: {{ order.warehouse }} · Статус: {{ order.status }}</p>

  <form method="post" class="inline">
    <input type="hidden" name="action" value="add-item" />
    <div>
      <label>Товар</label><br />
      <select name="product_id" required>
        {% for product in products %}
          <option value="{{ product.id }}">{{ product.sku }} — {{ product.name }}</option>
        {% endfor %}
      </select>
    </div>
    <div>
      <label>Количество</label><br />
      <input type="number" name="quantity" step="0.01" required />
    </div>
    <button type="submit">Добавить позицию</button>
  </form>

  <h3>Позиции</h3>
  <table>
    <thead>
      <tr>
        <th>SKU</th>
        <th>Товар</th>
        <th>Ед.</th>
        <th>Кол-во</th>
      </tr>
    </thead>
    <tbody>
      {% for item in items %}
        <tr>
          <td>{{ item.sku }}</td>
          <td>{{ item.name }}</td>
          <td>{{ item.unit }}</td>
          <td>{{ item.quantity }}</td>
        </tr>
      {% else %}
        <tr>
          <td colspan="4" class="muted">Нет позиций</td>
        </tr>
      {% endfor %}
    </tbody>
  </table>

  <form method="post" style="margin-top: 16px;">
    <input type="hidden" name="action" value="ship" />
    <button type="submit">Отгрузить</button>
  </form>
{% endblock %}
""",
    "inventory": """\
{% extends "base" %}
{% block content %}
  <h2>Остатки</h2>
  <form method="get" class="inline">
    <div>
      <label>Склад</label><br />
      <select name="warehouse_id">
        <option value="">Все</option>
        {% for warehouse in warehouses %}
          <option value="{{ warehouse.id }}" {% if selected_warehouse == warehouse.id %}selected{% endif %}>
            {{ warehouse.name }} ({{ warehouse.location }})
          </option>
        {% endfor %}
      </select>
    </div>
    <button type="submit">Показать</button>
  </form>

  <table>
    <thead>
      <tr>
        <th>Склад</th>
        <th>SKU</th>
        <th>Товар</th>
        <th>Ед.</th>
        <th>Кол-во</th>
      </tr>
    </thead>
    <tbody>
      {% for row in rows %}
        <tr>
          <td>{{ row['warehouse'] }}</td>
          <td>{{ row['sku'] }}</td>
          <td>{{ row['product'] }}</td>
          <td>{{ row['unit'] }}</td>
          <td>{{ row['quantity'] }}</td>
        </tr>
      {% else %}
        <tr>
          <td colspan="5" class="muted">Нет остатков</td>
        </tr>
      {% endfor %}
    </tbody>
  </table>
{% endblock %}
""",
    "movements": """\
{% extends "base" %}
{% block content %}
  <h2>Движения</h2>
  <form method="get" class="inline">
    <div>
      <label>Лимит</label><br />
      <input type="number" name="limit" value="{{ limit }}" min="1" max="500" />
    </div>
    <button type="submit">Показать</button>
  </form>

  <table>
    <thead>
      <tr>
        <th>Дата</th>
        <th>Склад</th>
        <th>SKU</th>
        <th>Товар</th>
        <th>Изменение</th>
        <th>Причина</th>
        <th>Тип</th>
        <th>ID</th>
      </tr>
    </thead>
    <tbody>
      {% for row in rows %}
        <tr>
          <td>{{ row['created_at'] }}</td>
          <td>{{ row['warehouse'] }}</td>
          <td>{{ row['sku'] }}</td>
          <td>{{ row['product'] }}</td>
          <td>{{ row['quantity_change'] }}</td>
          <td>{{ row['reason'] }}</td>
          <td>{{ row['ref_type'] }}</td>
          <td>{{ row['ref_id'] }}</td>
        </tr>
      {% else %}
        <tr>
          <td colspan="8" class="muted">Нет движений</td>
        </tr>
      {% endfor %}
    </tbody>
  </table>
{% endblock %}
""",
}


def render_template(name: str, **context: object) -> str:
    return render_template_string(TEMPLATES[name], **context)


def create_app(db_path: Optional[str] = None) -> Flask:
    app = Flask(__name__)
    app.config["DB_PATH"] = db_path or os.environ.get("WAREHOUSE_DB", DB_PATH_DEFAULT)

    def get_conn() -> sqlite3.Connection:
        conn = connect(app.config["DB_PATH"])
        init_db(conn)
        return conn

    @app.route("/")
    def index():
        conn = get_conn()
        return render_template(
            "index",
            clients=list_clients(conn),
            warehouses=list_warehouses(conn),
            products=list_products(conn),
            inventory_rows=inventory_report(conn),
            movement_rows=movement_report(conn, limit=10),
        )

    @app.route("/clients", methods=["GET", "POST"])
    def clients_page():
        conn = get_conn()
        if request.method == "POST":
            name = request.form.get("name", "").strip()
            if name:
                add_client(conn, name)
            return redirect(url_for("clients_page"))
        return render_template("clients", clients=list_clients(conn))

    @app.route("/warehouses", methods=["GET", "POST"])
    def warehouses_page():
        conn = get_conn()
        if request.method == "POST":
            name = request.form.get("name", "").strip()
            location = request.form.get("location", "").strip()
            if name and location:
                add_warehouse(conn, name, location)
            return redirect(url_for("warehouses_page"))
        return render_template("warehouses", warehouses=list_warehouses(conn))

    @app.route("/products", methods=["GET", "POST"])
    def products_page():
        conn = get_conn()
        if request.method == "POST":
            sku = request.form.get("sku", "").strip()
            name = request.form.get("name", "").strip()
            unit = request.form.get("unit", "").strip()
            if sku and name and unit:
                add_product(conn, sku, name, unit)
            return redirect(url_for("products_page"))
        return render_template("products", products=list_products(conn))

    @app.route("/inbound/new", methods=["GET", "POST"])
    def inbound_new():
        conn = get_conn()
        clients = list_clients(conn)
        warehouses = list_warehouses(conn)
        if request.method == "POST":
            client_id = int(request.form["client_id"])
            warehouse_id = int(request.form["warehouse_id"])
            order_id = create_inbound_order(conn, client_id, warehouse_id)
            return redirect(url_for("inbound_detail", order_id=order_id))
        return render_template("inbound_new", clients=clients, warehouses=warehouses)

    @app.route("/inbound/<int:order_id>", methods=["GET", "POST"])
    def inbound_detail(order_id: int):
        conn = get_conn()
        products = list_products(conn)
        if request.method == "POST":
            action = request.form.get("action")
            if action == "add-item":
                product_id = int(request.form["product_id"])
                quantity = float(request.form["quantity"])
                add_inbound_item(conn, order_id, product_id, quantity)
            elif action == "receive":
                receive_inbound_order(conn, order_id)
            return redirect(url_for("inbound_detail", order_id=order_id))

        order = conn.execute(
            "SELECT o.id, o.status, o.created_at, c.name AS client, w.name AS warehouse "
            "FROM inbound_orders o "
            "JOIN clients c ON c.id = o.client_id "
            "JOIN warehouses w ON w.id = o.warehouse_id "
            "WHERE o.id = ?",
            (order_id,),
        ).fetchone()
        items = conn.execute(
            "SELECT i.id, p.sku, p.name, p.unit, i.quantity "
            "FROM inbound_items i "
            "JOIN products p ON p.id = i.product_id "
            "WHERE i.inbound_order_id = ?",
            (order_id,),
        ).fetchall()
        return render_template("inbound_detail", order=order, items=items, products=products)

    @app.route("/outbound/new", methods=["GET", "POST"])
    def outbound_new():
        conn = get_conn()
        clients = list_clients(conn)
        warehouses = list_warehouses(conn)
        if request.method == "POST":
            client_id = int(request.form["client_id"])
            warehouse_id = int(request.form["warehouse_id"])
            order_id = create_outbound_order(conn, client_id, warehouse_id)
            return redirect(url_for("outbound_detail", order_id=order_id))
        return render_template("outbound_new", clients=clients, warehouses=warehouses)

    @app.route("/outbound/<int:order_id>", methods=["GET", "POST"])
    def outbound_detail(order_id: int):
        conn = get_conn()
        products = list_products(conn)
        if request.method == "POST":
            action = request.form.get("action")
            if action == "add-item":
                product_id = int(request.form["product_id"])
                quantity = float(request.form["quantity"])
                add_outbound_item(conn, order_id, product_id, quantity)
            elif action == "ship":
                ship_outbound_order(conn, order_id)
            return redirect(url_for("outbound_detail", order_id=order_id))

        order = conn.execute(
            "SELECT o.id, o.status, o.created_at, c.name AS client, w.name AS warehouse "
            "FROM outbound_orders o "
            "JOIN clients c ON c.id = o.client_id "
            "JOIN warehouses w ON w.id = o.warehouse_id "
            "WHERE o.id = ?",
            (order_id,),
        ).fetchone()
        items = conn.execute(
            "SELECT i.id, p.sku, p.name, p.unit, i.quantity "
            "FROM outbound_items i "
            "JOIN products p ON p.id = i.product_id "
            "WHERE i.outbound_order_id = ?",
            (order_id,),
        ).fetchall()
        return render_template("outbound_detail", order=order, items=items, products=products)

    @app.route("/inventory")
    def inventory_page():
        conn = get_conn()
        warehouse_id = request.args.get("warehouse_id", type=int)
        return render_template(
            "inventory",
            rows=inventory_report(conn, warehouse_id),
            warehouses=list_warehouses(conn),
            selected_warehouse=warehouse_id,
        )

    @app.route("/movements")
    def movements_page():
        conn = get_conn()
        limit = request.args.get("limit", default=50, type=int)
        return render_template("movements", rows=movement_report(conn, limit), limit=limit)

    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=8000, debug=True)
