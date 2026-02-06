#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WMS - Складська Система Управління
Enterprise-рівень система для 3PL операцій
Версія: 1.0.0
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog
import sqlite3
import uuid
import hashlib
import os
import sys
import csv
import json
import tempfile
import webbrowser
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from contextlib import contextmanager
import base64
import threading

# ============================================================================
# КОНСТАНТИ ТА ЛОКАЛІЗАЦІЯ
# ============================================================================

APP_NAME = "WMS - Складська Система"
APP_VERSION = "1.0.0"
DB_FILE = "wms.sqlite"
LOG_FILE = "wms.log"

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DocStatus(Enum):
    DRAFT = "Чернетка"
    IN_PROGRESS = "В роботі"
    POSTED = "Проведено"
    CANCELLED = "Скасовано"

class MoveType(Enum):
    IN_RECEIPT = "Прихід"
    PUTAWAY = "Розміщення"
    MOVE = "Переміщення"
    RESERVE = "Резерв"
    UNRESERVE = "Зняття резерву"
    PICK = "Відбір"
    PACK = "Пакування"
    SHIP = "Відвантаження"
    RETURN = "Повернення"
    WRITE_OFF = "Списання"
    ADJUSTMENT = "Коригування"

class ZoneType(Enum):
    RECEIVING = "Приймання"
    STORAGE = "Зберігання"
    PICKING = "Комплектація"
    PACKING = "Пакування"
    SHIPPING = "Відвантаження"
    QUARANTINE = "Карантин"
    DAMAGE = "Брак"
    RETURNS = "Повернення"

class LocationType(Enum):
    FLOOR = "Підлога"
    SHELF = "Полиця"
    PALLET = "Палетне місце"
    MEZZANINE = "Мезонін"

# Кольори UI (стиль 1С)
COLORS = {
    'bg': '#F0F0F0',
    'menu_bg': '#E8E8E8',
    'toolbar_bg': '#D0D0D0',
    'white': '#FFFFFF',
    'border': '#A0A0A0',
    'header': '#4472C4',
    'header_text': '#FFFFFF',
    'row_alt': '#F5F5F5',
    'selected': '#CCE5FF',
    'btn_primary': '#4472C4',
    'btn_success': '#70AD47',
    'btn_danger': '#C00000',
    'btn_warning': '#FFC000',
    'text': '#333333',
}

# Права доступу
PERMISSIONS = [
    'users_view', 'users_edit', 'users_delete',
    'roles_view', 'roles_edit',
    'items_view', 'items_edit', 'items_delete', 'items_import',
    'clients_view', 'clients_edit', 'clients_delete',
    'suppliers_view', 'suppliers_edit', 'suppliers_delete',
    'warehouses_view', 'warehouses_edit',
    'zones_view', 'zones_edit',
    'locations_view', 'locations_edit',
    'inbound_view', 'inbound_edit', 'inbound_post',
    'outbound_view', 'outbound_edit', 'outbound_post',
    'inventory_view', 'inventory_edit', 'inventory_post',
    'returns_view', 'returns_edit', 'returns_post',
    'writeoffs_view', 'writeoffs_edit', 'writeoffs_post',
    'reports_view', 'reports_export',
    'audit_view',
    'settings_view', 'settings_edit',
]

# ============================================================================
# УТИЛІТИ
# ============================================================================

def generate_uuid() -> str:
    """Генерація UUID"""
    return str(uuid.uuid4())

def now_str() -> str:
    """Поточна дата-час як рядок"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def today_str() -> str:
    """Поточна дата як рядок"""
    return datetime.now().strftime('%Y-%m-%d')

def hash_password(password: str, salt: str = None) -> Tuple[str, str]:
    """Хешування пароля з сіллю"""
    if salt is None:
        salt = os.urandom(32).hex()
    pwd_hash = hashlib.pbkdf2_hmac(
        'sha256',
        password.encode('utf-8'),
        salt.encode('utf-8'),
        100000
    ).hex()
    return pwd_hash, salt

def verify_password(password: str, pwd_hash: str, salt: str) -> bool:
    """Перевірка пароля"""
    check_hash, _ = hash_password(password, salt)
    return check_hash == pwd_hash

def generate_doc_number(prefix: str) -> str:
    """Генерація номера документа"""
    ts = datetime.now().strftime('%Y%m%d%H%M%S')
    rand = uuid.uuid4().hex[:4].upper()
    return f"{prefix}-{ts}-{rand}"

# ============================================================================
# DATABASE MANAGER
# ============================================================================

class DatabaseManager:
    """Менеджер бази даних SQLite"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self.db_path = DB_FILE
        self._local = threading.local()
        self.init_database()
    
    @property
    def conn(self):
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA foreign_keys = ON")
            self._local.conn.execute("PRAGMA journal_mode = WAL")
        return self._local.conn
    
    @contextmanager
    def transaction(self):
        """Контекстний менеджер для транзакцій"""
        try:
            yield self.conn
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Transaction error: {e}")
            raise
    
    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        """Виконання SQL запиту"""
        return self.conn.execute(sql, params)
    
    def executemany(self, sql: str, params_list: list) -> sqlite3.Cursor:
        """Виконання багатьох SQL запитів"""
        return self.conn.executemany(sql, params_list)
    
    def fetchone(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        """Отримання одного запису"""
        return self.conn.execute(sql, params).fetchone()
    
    def fetchall(self, sql: str, params: tuple = ()) -> List[sqlite3.Row]:
        """Отримання всіх записів"""
        return self.conn.execute(sql, params).fetchall()
    
    def commit(self):
        """Підтвердження транзакції"""
        self.conn.commit()
    
    def init_database(self):
        """Ініціалізація схеми бази даних"""
        logger.info("Initializing database...")
        
        schema = '''
        -- Ролі користувачів
        CREATE TABLE IF NOT EXISTS roles (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            description TEXT,
            is_system INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Права ролей
        CREATE TABLE IF NOT EXISTS role_permissions (
            id TEXT PRIMARY KEY,
            role_id TEXT NOT NULL,
            permission TEXT NOT NULL,
            FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE,
            UNIQUE(role_id, permission)
        );
        
        -- Користувачі
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            password_salt TEXT NOT NULL,
            full_name TEXT,
            email TEXT,
            role_id TEXT,
            client_id TEXT,
            is_active INTEGER DEFAULT 1,
            failed_attempts INTEGER DEFAULT 0,
            locked_until TEXT,
            last_login TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (role_id) REFERENCES roles(id)
        );
        
        -- Склади
        CREATE TABLE IF NOT EXISTS warehouses (
            id TEXT PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            address TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Зони складу
        CREATE TABLE IF NOT EXISTS zones (
            id TEXT PRIMARY KEY,
            warehouse_id TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            zone_type TEXT NOT NULL,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
            UNIQUE(warehouse_id, code)
        );
        
        -- Комірки (локації)
        CREATE TABLE IF NOT EXISTS locations (
            id TEXT PRIMARY KEY,
            zone_id TEXT NOT NULL,
            code TEXT NOT NULL,
            location_type TEXT NOT NULL,
            max_weight REAL DEFAULT 0,
            max_volume REAL DEFAULT 0,
            max_pallets INTEGER DEFAULT 1,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (zone_id) REFERENCES zones(id),
            UNIQUE(zone_id, code)
        );
        
        -- Клієнти 3PL
        CREATE TABLE IF NOT EXISTS clients (
            id TEXT PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            legal_name TEXT,
            tax_id TEXT,
            address TEXT,
            phone TEXT,
            email TEXT,
            contract_number TEXT,
            contract_date TEXT,
            sla_days INTEGER DEFAULT 3,
            tariff_type TEXT DEFAULT 'operations',
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Постачальники
        CREATE TABLE IF NOT EXISTS suppliers (
            id TEXT PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            legal_name TEXT,
            tax_id TEXT,
            address TEXT,
            phone TEXT,
            email TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Транспортні компанії
        CREATE TABLE IF NOT EXISTS carriers (
            id TEXT PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            phone TEXT,
            email TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Категорії товарів
        CREATE TABLE IF NOT EXISTS item_categories (
            id TEXT PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            parent_id TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (parent_id) REFERENCES item_categories(id)
        );
        
        -- Одиниці виміру
        CREATE TABLE IF NOT EXISTS uom (
            id TEXT PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Номенклатура (товари)
        CREATE TABLE IF NOT EXISTS items (
            id TEXT PRIMARY KEY,
            sku TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            name_en TEXT,
            description TEXT,
            category_id TEXT,
            uom_id TEXT,
            alt_uom_id TEXT,
            alt_uom_ratio REAL DEFAULT 1,
            weight REAL DEFAULT 0,
            volume REAL DEFAULT 0,
            length REAL DEFAULT 0,
            width REAL DEFAULT 0,
            height REAL DEFAULT 0,
            is_serialized INTEGER DEFAULT 0,
            is_batch_tracked INTEGER DEFAULT 0,
            is_expiry_tracked INTEGER DEFAULT 0,
            min_expiry_days INTEGER DEFAULT 0,
            storage_temp_min REAL,
            storage_temp_max REAL,
            storage_rules TEXT,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES item_categories(id),
            FOREIGN KEY (uom_id) REFERENCES uom(id),
            FOREIGN KEY (alt_uom_id) REFERENCES uom(id)
        );
        
        -- Штрихкоди товарів
        CREATE TABLE IF NOT EXISTS item_barcodes (
            id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            barcode TEXT NOT NULL UNIQUE,
            barcode_type TEXT DEFAULT 'EAN13',
            is_primary INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (item_id) REFERENCES items(id) ON DELETE CASCADE
        );
        
        -- Фото товарів
        CREATE TABLE IF NOT EXISTS item_photos (
            id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            photo_data TEXT,
            is_primary INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (item_id) REFERENCES items(id) ON DELETE CASCADE
        );
        
        -- Залишки (агреговані)
        CREATE TABLE IF NOT EXISTS inventory_balances (
            id TEXT PRIMARY KEY,
            client_id TEXT NOT NULL,
            warehouse_id TEXT NOT NULL,
            location_id TEXT,
            item_id TEXT NOT NULL,
            batch_number TEXT,
            serial_number TEXT,
            expiry_date TEXT,
            qty_available REAL DEFAULT 0,
            qty_reserved REAL DEFAULT 0,
            qty_blocked REAL DEFAULT 0,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
            FOREIGN KEY (location_id) REFERENCES locations(id),
            FOREIGN KEY (item_id) REFERENCES items(id),
            UNIQUE(client_id, warehouse_id, location_id, item_id, batch_number, serial_number, expiry_date)
        );
        
        -- Рухи товарів (журнал)
        CREATE TABLE IF NOT EXISTS stock_moves (
            id TEXT PRIMARY KEY,
            move_type TEXT NOT NULL,
            doc_type TEXT,
            doc_id TEXT,
            client_id TEXT NOT NULL,
            warehouse_id TEXT NOT NULL,
            from_location_id TEXT,
            to_location_id TEXT,
            item_id TEXT NOT NULL,
            batch_number TEXT,
            serial_number TEXT,
            expiry_date TEXT,
            qty REAL NOT NULL,
            user_id TEXT,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
            FOREIGN KEY (from_location_id) REFERENCES locations(id),
            FOREIGN KEY (to_location_id) REFERENCES locations(id),
            FOREIGN KEY (item_id) REFERENCES items(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        -- Замовлення на приймання (Inbound)
        CREATE TABLE IF NOT EXISTS inbound_orders (
            id TEXT PRIMARY KEY,
            doc_number TEXT NOT NULL UNIQUE,
            doc_date TEXT NOT NULL,
            status TEXT DEFAULT 'Чернетка',
            client_id TEXT NOT NULL,
            supplier_id TEXT,
            warehouse_id TEXT NOT NULL,
            expected_date TEXT,
            notes TEXT,
            created_by TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            posted_at TEXT,
            posted_by TEXT,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (supplier_id) REFERENCES suppliers(id),
            FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
        
        -- Позиції замовлення на приймання
        CREATE TABLE IF NOT EXISTS inbound_order_lines (
            id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            line_number INTEGER NOT NULL,
            item_id TEXT NOT NULL,
            expected_qty REAL NOT NULL,
            received_qty REAL DEFAULT 0,
            batch_number TEXT,
            expiry_date TEXT,
            notes TEXT,
            FOREIGN KEY (order_id) REFERENCES inbound_orders(id) ON DELETE CASCADE,
            FOREIGN KEY (item_id) REFERENCES items(id)
        );
        
        -- Замовлення клієнта (Outbound)
        CREATE TABLE IF NOT EXISTS outbound_orders (
            id TEXT PRIMARY KEY,
            doc_number TEXT NOT NULL UNIQUE,
            doc_date TEXT NOT NULL,
            status TEXT DEFAULT 'Чернетка',
            client_id TEXT NOT NULL,
            warehouse_id TEXT NOT NULL,
            carrier_id TEXT,
            delivery_address TEXT,
            delivery_date TEXT,
            priority INTEGER DEFAULT 5,
            notes TEXT,
            created_by TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            posted_at TEXT,
            posted_by TEXT,
            shipped_at TEXT,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
            FOREIGN KEY (carrier_id) REFERENCES carriers(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
        
        -- Позиції замовлення клієнта
        CREATE TABLE IF NOT EXISTS outbound_order_lines (
            id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            line_number INTEGER NOT NULL,
            item_id TEXT NOT NULL,
            ordered_qty REAL NOT NULL,
            reserved_qty REAL DEFAULT 0,
            picked_qty REAL DEFAULT 0,
            shipped_qty REAL DEFAULT 0,
            batch_number TEXT,
            expiry_date TEXT,
            notes TEXT,
            FOREIGN KEY (order_id) REFERENCES outbound_orders(id) ON DELETE CASCADE,
            FOREIGN KEY (item_id) REFERENCES items(id)
        );
        
        -- Інвентаризації
        CREATE TABLE IF NOT EXISTS inventory_counts (
            id TEXT PRIMARY KEY,
            doc_number TEXT NOT NULL UNIQUE,
            doc_date TEXT NOT NULL,
            status TEXT DEFAULT 'Чернетка',
            client_id TEXT,
            warehouse_id TEXT NOT NULL,
            zone_id TEXT,
            count_type TEXT DEFAULT 'full',
            notes TEXT,
            created_by TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            posted_at TEXT,
            posted_by TEXT,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
            FOREIGN KEY (zone_id) REFERENCES zones(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
        
        -- Позиції інвентаризації
        CREATE TABLE IF NOT EXISTS inventory_count_lines (
            id TEXT PRIMARY KEY,
            count_id TEXT NOT NULL,
            location_id TEXT,
            item_id TEXT NOT NULL,
            batch_number TEXT,
            expiry_date TEXT,
            system_qty REAL DEFAULT 0,
            counted_qty REAL DEFAULT 0,
            difference REAL DEFAULT 0,
            notes TEXT,
            FOREIGN KEY (count_id) REFERENCES inventory_counts(id) ON DELETE CASCADE,
            FOREIGN KEY (location_id) REFERENCES locations(id),
            FOREIGN KEY (item_id) REFERENCES items(id)
        );
        
        -- Повернення
        CREATE TABLE IF NOT EXISTS returns (
            id TEXT PRIMARY KEY,
            doc_number TEXT NOT NULL UNIQUE,
            doc_date TEXT NOT NULL,
            status TEXT DEFAULT 'Чернетка',
            return_type TEXT DEFAULT 'customer',
            client_id TEXT NOT NULL,
            warehouse_id TEXT NOT NULL,
            original_order_id TEXT,
            notes TEXT,
            created_by TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            posted_at TEXT,
            posted_by TEXT,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
            FOREIGN KEY (original_order_id) REFERENCES outbound_orders(id),
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
        
        -- Позиції повернення
        CREATE TABLE IF NOT EXISTS return_lines (
            id TEXT PRIMARY KEY,
            return_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            qty REAL NOT NULL,
            condition TEXT DEFAULT 'good',
            destination_zone TEXT DEFAULT 'STORAGE',
            batch_number TEXT,
            notes TEXT,
            FOREIGN KEY (return_id) REFERENCES returns(id) ON DELETE CASCADE,
            FOREIGN KEY (item_id) REFERENCES items(id)
        );
        
        -- Списання
        CREATE TABLE IF NOT EXISTS writeoffs (
            id TEXT PRIMARY KEY,
            doc_number TEXT NOT NULL UNIQUE,
            doc_date TEXT NOT NULL,
            status TEXT DEFAULT 'Чернетка',
            client_id TEXT NOT NULL,
            warehouse_id TEXT NOT NULL,
            reason TEXT,
            notes TEXT,
            created_by TEXT,
            approved_by TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            posted_at TEXT,
            posted_by TEXT,
            FOREIGN KEY (client_id) REFERENCES clients(id),
            FOREIGN KEY (warehouse_id) REFERENCES warehouses(id),
            FOREIGN KEY (created_by) REFERENCES users(id),
            FOREIGN KEY (approved_by) REFERENCES users(id)
        );
        
        -- Позиції списання
        CREATE TABLE IF NOT EXISTS writeoff_lines (
            id TEXT PRIMARY KEY,
            writeoff_id TEXT NOT NULL,
            location_id TEXT,
            item_id TEXT NOT NULL,
            qty REAL NOT NULL,
            batch_number TEXT,
            reason TEXT,
            notes TEXT,
            FOREIGN KEY (writeoff_id) REFERENCES writeoffs(id) ON DELETE CASCADE,
            FOREIGN KEY (location_id) REFERENCES locations(id),
            FOREIGN KEY (item_id) REFERENCES items(id)
        );
        
        -- Журнал аудиту
        CREATE TABLE IF NOT EXISTS audit_log (
            id TEXT PRIMARY KEY,
            user_id TEXT,
            username TEXT,
            action TEXT NOT NULL,
            entity_type TEXT,
            entity_id TEXT,
            old_values TEXT,
            new_values TEXT,
            ip_address TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Індекси
        CREATE INDEX IF NOT EXISTS idx_inventory_balances_client ON inventory_balances(client_id);
        CREATE INDEX IF NOT EXISTS idx_inventory_balances_warehouse ON inventory_balances(warehouse_id);
        CREATE INDEX IF NOT EXISTS idx_inventory_balances_item ON inventory_balances(item_id);
        CREATE INDEX IF NOT EXISTS idx_inventory_balances_location ON inventory_balances(location_id);
        CREATE INDEX IF NOT EXISTS idx_stock_moves_client ON stock_moves(client_id);
        CREATE INDEX IF NOT EXISTS idx_stock_moves_item ON stock_moves(item_id);
        CREATE INDEX IF NOT EXISTS idx_stock_moves_doc ON stock_moves(doc_type, doc_id);
        CREATE INDEX IF NOT EXISTS idx_stock_moves_date ON stock_moves(created_at);
        CREATE INDEX IF NOT EXISTS idx_inbound_orders_client ON inbound_orders(client_id);
        CREATE INDEX IF NOT EXISTS idx_inbound_orders_status ON inbound_orders(status);
        CREATE INDEX IF NOT EXISTS idx_outbound_orders_client ON outbound_orders(client_id);
        CREATE INDEX IF NOT EXISTS idx_outbound_orders_status ON outbound_orders(status);
        CREATE INDEX IF NOT EXISTS idx_items_sku ON items(sku);
        CREATE INDEX IF NOT EXISTS idx_item_barcodes_barcode ON item_barcodes(barcode);
        CREATE INDEX IF NOT EXISTS idx_audit_log_user ON audit_log(user_id);
        CREATE INDEX IF NOT EXISTS idx_audit_log_entity ON audit_log(entity_type, entity_id);
        CREATE INDEX IF NOT EXISTS idx_audit_log_date ON audit_log(created_at);
        '''
        
        with self.transaction():
            self.conn.executescript(schema)
        
        self._init_default_data()
        logger.info("Database initialized successfully")
    
    def _init_default_data(self):
        """Ініціалізація початкових даних"""
        # Перевірка чи є дані
        if self.fetchone("SELECT 1 FROM roles LIMIT 1"):
            return
        
        logger.info("Creating default data...")
        
        with self.transaction():
            # Ролі
            roles_data = [
                (generate_uuid(), 'Адміністратор', 'Повний доступ до системи', 1),
                (generate_uuid(), 'Комірник', 'Складські операції', 1),
                (generate_uuid(), 'Супервайзер складу', 'Управління складом', 1),
                (generate_uuid(), 'Менеджер логістики', 'Логістичні операції', 1),
                (generate_uuid(), 'Клієнт 3PL', 'Обмежений доступ до своїх даних', 1),
                (generate_uuid(), 'Бухгалтер', 'Звіти та документи', 1),
            ]
            self.executemany(
                "INSERT INTO roles (id, name, description, is_system) VALUES (?,?,?,?)",
                roles_data
            )
            
            # Права для адміністратора (всі права)
            admin_role = self.fetchone("SELECT id FROM roles WHERE name = 'Адміністратор'")
            for perm in PERMISSIONS:
                self.execute(
                    "INSERT INTO role_permissions (id, role_id, permission) VALUES (?,?,?)",
                    (generate_uuid(), admin_role['id'], perm)
                )
            
            # Права для комірника
            warehouse_role = self.fetchone("SELECT id FROM roles WHERE name = 'Комірник'")
            warehouse_perms = ['items_view', 'inbound_view', 'inbound_edit', 'outbound_view', 
                             'outbound_edit', 'inventory_view', 'inventory_edit', 'locations_view']
            for perm in warehouse_perms:
                self.execute(
                    "INSERT INTO role_permissions (id, role_id, permission) VALUES (?,?,?)",
                    (generate_uuid(), warehouse_role['id'], perm)
                )
            
            # Одиниці виміру
            uom_data = [
                (generate_uuid(), 'шт', 'Штука'),
                (generate_uuid(), 'кг', 'Кілограм'),
                (generate_uuid(), 'л', 'Літр'),
                (generate_uuid(), 'м', 'Метр'),
                (generate_uuid(), 'уп', 'Упаковка'),
                (generate_uuid(), 'кор', 'Коробка'),
                (generate_uuid(), 'пал', 'Палета'),
            ]
            self.executemany(
                "INSERT INTO uom (id, code, name) VALUES (?,?,?)",
                uom_data
            )
            
            # Категорії товарів
            cat_data = [
                (generate_uuid(), 'FOOD', 'Продукти харчування', None),
                (generate_uuid(), 'ELECTRONICS', 'Електроніка', None),
                (generate_uuid(), 'CLOTHING', 'Одяг', None),
                (generate_uuid(), 'HOUSEHOLD', 'Побутові товари', None),
                (generate_uuid(), 'OTHER', 'Інше', None),
            ]
            self.executemany(
                "INSERT INTO item_categories (id, code, name, parent_id) VALUES (?,?,?,?)",
                cat_data
            )
            
            # Адмін користувач
            pwd_hash, salt = hash_password('admin123')
            admin_user_id = generate_uuid()
            self.execute(
                '''INSERT INTO users (id, username, password_hash, password_salt, 
                   full_name, role_id, is_active) VALUES (?,?,?,?,?,?,?)''',
                (admin_user_id, 'admin', pwd_hash, salt, 'Адміністратор системи', 
                 admin_role['id'], 1)
            )
            
            # Склад
            warehouse_id = generate_uuid()
            self.execute(
                "INSERT INTO warehouses (id, code, name, address) VALUES (?,?,?,?)",
                (warehouse_id, 'WH01', 'Головний склад', 'м. Київ, вул. Складська, 1')
            )
            
            # Зони
            zones_data = [
                (generate_uuid(), warehouse_id, 'RCV', 'Зона приймання', ZoneType.RECEIVING.value),
                (generate_uuid(), warehouse_id, 'STR-A', 'Зона зберігання A', ZoneType.STORAGE.value),
                (generate_uuid(), warehouse_id, 'STR-B', 'Зона зберігання B', ZoneType.STORAGE.value),
                (generate_uuid(), warehouse_id, 'PCK', 'Зона комплектації', ZoneType.PICKING.value),
                (generate_uuid(), warehouse_id, 'SHP', 'Зона відвантаження', ZoneType.SHIPPING.value),
                (generate_uuid(), warehouse_id, 'QRN', 'Карантин', ZoneType.QUARANTINE.value),
                (generate_uuid(), warehouse_id, 'DMG', 'Брак', ZoneType.DAMAGE.value),
            ]
            self.executemany(
                "INSERT INTO zones (id, warehouse_id, code, name, zone_type) VALUES (?,?,?,?,?)",
                zones_data
            )
            
            # Комірки
            storage_zone = self.fetchone(
                "SELECT id FROM zones WHERE warehouse_id = ? AND code = 'STR-A'", 
                (warehouse_id,)
            )
            locations_data = []
            for row in range(1, 6):
                for level in range(1, 4):
                    for pos in range(1, 4):
                        loc_id = generate_uuid()
                        code = f"A-{row:02d}-{level:02d}-{pos:02d}"
                        locations_data.append(
                            (loc_id, storage_zone['id'], code, LocationType.SHELF.value, 
                             100.0, 1.0, 1)
                        )
            self.executemany(
                '''INSERT INTO locations (id, zone_id, code, location_type, 
                   max_weight, max_volume, max_pallets) VALUES (?,?,?,?,?,?,?)''',
                locations_data
            )
            
            # Клієнт 3PL
            client_id = generate_uuid()
            self.execute(
                '''INSERT INTO clients (id, code, name, legal_name, tax_id, address, 
                   phone, email, contract_number, sla_days, tariff_type) 
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                (client_id, 'CL001', 'ТОВ "Торгівля Плюс"', 'ТОВ "Торгівля Плюс"',
                 '12345678', 'м. Київ, вул. Хрещатик, 1', '+380441234567',
                 'info@trade.ua', 'Д-2024-001', 3, 'operations')
            )
            
            # Постачальник
            supplier_id = generate_uuid()
            self.execute(
                '''INSERT INTO suppliers (id, code, name, legal_name, tax_id, address, 
                   phone, email) VALUES (?,?,?,?,?,?,?,?)''',
                (supplier_id, 'SUP001', 'ТОВ "Постачальник"', 'ТОВ "Постачальник"',
                 '87654321', 'м. Львів, вул. Промислова, 5', '+380321234567',
                 'info@supplier.ua')
            )
            
            # Перевізник
            carrier_id = generate_uuid()
            self.execute(
                "INSERT INTO carriers (id, code, name, phone, email) VALUES (?,?,?,?,?)",
                (carrier_id, 'CAR001', 'Нова Пошта', '+380800500609', 'info@novaposhta.ua')
            )
            
            # Одиниця виміру за замовчуванням
            uom = self.fetchone("SELECT id FROM uom WHERE code = 'шт'")
            category = self.fetchone("SELECT id FROM item_categories WHERE code = 'OTHER'")
            
            # Товари
            items_data = []
            for i in range(1, 11):
                item_id = generate_uuid()
                items_data.append((
                    item_id, f'SKU-{i:04d}', f'Товар {i}', f'Item {i}',
                    f'Опис товару {i}', category['id'], uom['id'],
                    round(0.5 + i * 0.1, 2), round(0.001 + i * 0.0005, 4),
                    0, 0, 0, 1
                ))
            self.executemany(
                '''INSERT INTO items (id, sku, name, name_en, description, category_id, 
                   uom_id, weight, volume, is_serialized, is_batch_tracked, 
                   is_expiry_tracked, is_active) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                items_data
            )
            
            # Штрихкоди
            items = self.fetchall("SELECT id, sku FROM items")
            for item in items:
                barcode = f'482{item["sku"].replace("-", "")}'
                self.execute(
                    "INSERT INTO item_barcodes (id, item_id, barcode, is_primary) VALUES (?,?,?,?)",
                    (generate_uuid(), item['id'], barcode, 1)
                )
            
            # Демо замовлення на приймання
            first_item = self.fetchone("SELECT id FROM items LIMIT 1")
            inbound_id = generate_uuid()
            self.execute(
                '''INSERT INTO inbound_orders (id, doc_number, doc_date, status, client_id, 
                   supplier_id, warehouse_id, expected_date, created_by) 
                   VALUES (?,?,?,?,?,?,?,?,?)''',
                (inbound_id, generate_doc_number('IN'), today_str(), DocStatus.DRAFT.value,
                 client_id, supplier_id, warehouse_id, 
                 (datetime.now() + timedelta(days=2)).strftime('%Y-%m-%d'), admin_user_id)
            )
            
            # Позиції замовлення на приймання
            items_for_order = self.fetchall("SELECT id FROM items LIMIT 5")
            for idx, item in enumerate(items_for_order, 1):
                self.execute(
                    '''INSERT INTO inbound_order_lines (id, order_id, line_number, item_id, 
                       expected_qty) VALUES (?,?,?,?,?)''',
                    (generate_uuid(), inbound_id, idx, item['id'], 100)
                )
            
            # Демо замовлення на відвантаження
            outbound_id = generate_uuid()
            self.execute(
                '''INSERT INTO outbound_orders (id, doc_number, doc_date, status, client_id, 
                   warehouse_id, carrier_id, delivery_address, delivery_date, priority, 
                   created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                (outbound_id, generate_doc_number('OUT'), today_str(), DocStatus.DRAFT.value,
                 client_id, warehouse_id, carrier_id, 'м. Одеса, вул. Морська, 10',
                 (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d'), 5, admin_user_id)
            )
            
            # Позиції замовлення на відвантаження
            for idx, item in enumerate(items_for_order[:3], 1):
                self.execute(
                    '''INSERT INTO outbound_order_lines (id, order_id, line_number, item_id, 
                       ordered_qty) VALUES (?,?,?,?,?)''',
                    (generate_uuid(), outbound_id, idx, item['id'], 10)
                )
        
        logger.info("Default data created successfully")


# ============================================================================
# DATA ACCESS LAYER (DAL)
# ============================================================================

class BaseDAL:
    """Базовий клас для доступу до даних"""
    
    def __init__(self):
        self.db = DatabaseManager()
    
    def _row_to_dict(self, row: sqlite3.Row) -> dict:
        """Конвертація Row в словник"""
        if row is None:
            return None
        return dict(row)
    
    def _rows_to_dicts(self, rows: List[sqlite3.Row]) -> List[dict]:
        """Конвертація списку Row в список словників"""
        return [dict(row) for row in rows]


class UserDAL(BaseDAL):
    """DAL для користувачів"""
    
    def get_by_id(self, user_id: str) -> Optional[dict]:
        row = self.db.fetchone(
            '''SELECT u.*, r.name as role_name 
               FROM users u LEFT JOIN roles r ON u.role_id = r.id 
               WHERE u.id = ?''',
            (user_id,)
        )
        return self._row_to_dict(row)
    
    def get_by_username(self, username: str) -> Optional[dict]:
        row = self.db.fetchone(
            '''SELECT u.*, r.name as role_name 
               FROM users u LEFT JOIN roles r ON u.role_id = r.id 
               WHERE u.username = ?''',
            (username,)
        )
        return self._row_to_dict(row)
    
    def get_all(self, active_only: bool = False) -> List[dict]:
        sql = '''SELECT u.*, r.name as role_name 
                 FROM users u LEFT JOIN roles r ON u.role_id = r.id'''
        if active_only:
            sql += ' WHERE u.is_active = 1'
        sql += ' ORDER BY u.username'
        return self._rows_to_dicts(self.db.fetchall(sql))
    
    def create(self, data: dict) -> str:
        user_id = generate_uuid()
        pwd_hash, salt = hash_password(data['password'])
        self.db.execute(
            '''INSERT INTO users (id, username, password_hash, password_salt, 
               full_name, email, role_id, client_id, is_active) 
               VALUES (?,?,?,?,?,?,?,?,?)''',
            (user_id, data['username'], pwd_hash, salt, data.get('full_name'),
             data.get('email'), data.get('role_id'), data.get('client_id'), 1)
        )
        self.db.commit()
        return user_id
    
    def update(self, user_id: str, data: dict):
        fields = []
        values = []
        for key in ['full_name', 'email', 'role_id', 'client_id', 'is_active']:
            if key in data:
                fields.append(f'{key} = ?')
                values.append(data[key])
        if 'password' in data and data['password']:
            pwd_hash, salt = hash_password(data['password'])
            fields.extend(['password_hash = ?', 'password_salt = ?'])
            values.extend([pwd_hash, salt])
        
        fields.append('updated_at = ?')
        values.append(now_str())
        values.append(user_id)
        
        self.db.execute(
            f"UPDATE users SET {', '.join(fields)} WHERE id = ?",
            tuple(values)
        )
        self.db.commit()
    
    def delete(self, user_id: str):
        self.db.execute("DELETE FROM users WHERE id = ?", (user_id,))
        self.db.commit()
    
    def update_login_attempt(self, user_id: str, success: bool):
        if success:
            self.db.execute(
                "UPDATE users SET failed_attempts = 0, last_login = ?, locked_until = NULL WHERE id = ?",
                (now_str(), user_id)
            )
        else:
            user = self.get_by_id(user_id)
            attempts = user['failed_attempts'] + 1
            locked_until = None
            if attempts >= 5:
                locked_until = (datetime.now() + timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
            self.db.execute(
                "UPDATE users SET failed_attempts = ?, locked_until = ? WHERE id = ?",
                (attempts, locked_until, user_id)
            )
        self.db.commit()
    
    def get_permissions(self, user_id: str) -> List[str]:
        rows = self.db.fetchall(
            '''SELECT rp.permission FROM role_permissions rp
               INNER JOIN users u ON u.role_id = rp.role_id
               WHERE u.id = ?''',
            (user_id,)
        )
        return [row['permission'] for row in rows]


class RoleDAL(BaseDAL):
    """DAL для ролей"""
    
    def get_all(self) -> List[dict]:
        return self._rows_to_dicts(
            self.db.fetchall("SELECT * FROM roles ORDER BY name")
        )
    
    def get_by_id(self, role_id: str) -> Optional[dict]:
        row = self.db.fetchone("SELECT * FROM roles WHERE id = ?", (role_id,))
        return self._row_to_dict(row)
    
    def get_permissions(self, role_id: str) -> List[str]:
        rows = self.db.fetchall(
            "SELECT permission FROM role_permissions WHERE role_id = ?",
            (role_id,)
        )
        return [row['permission'] for row in rows]
    
    def set_permissions(self, role_id: str, permissions: List[str]):
        with self.db.transaction():
            self.db.execute("DELETE FROM role_permissions WHERE role_id = ?", (role_id,))
            for perm in permissions:
                self.db.execute(
                    "INSERT INTO role_permissions (id, role_id, permission) VALUES (?,?,?)",
                    (generate_uuid(), role_id, perm)
                )


class ItemDAL(BaseDAL):
    """DAL для номенклатури"""
    
    def get_all(self, active_only: bool = True, search: str = None) -> List[dict]:
        sql = '''SELECT i.*, c.name as category_name, u.name as uom_name
                 FROM items i 
                 LEFT JOIN item_categories c ON i.category_id = c.id
                 LEFT JOIN uom u ON i.uom_id = u.id'''
        params = []
        conditions = []
        
        if active_only:
            conditions.append('i.is_active = 1')
        if search:
            conditions.append('(i.sku LIKE ? OR i.name LIKE ?)')
            params.extend([f'%{search}%', f'%{search}%'])
        
        if conditions:
            sql += ' WHERE ' + ' AND '.join(conditions)
        sql += ' ORDER BY i.sku'
        
        return self._rows_to_dicts(self.db.fetchall(sql, tuple(params)))
    
    def get_by_id(self, item_id: str) -> Optional[dict]:
        row = self.db.fetchone(
            '''SELECT i.*, c.name as category_name, u.name as uom_name
               FROM items i 
               LEFT JOIN item_categories c ON i.category_id = c.id
               LEFT JOIN uom u ON i.uom_id = u.id
               WHERE i.id = ?''',
            (item_id,)
        )
        return self._row_to_dict(row)
    
    def get_by_sku(self, sku: str) -> Optional[dict]:
        row = self.db.fetchone("SELECT * FROM items WHERE sku = ?", (sku,))
        return self._row_to_dict(row)
    
    def get_by_barcode(self, barcode: str) -> Optional[dict]:
        row = self.db.fetchone(
            '''SELECT i.* FROM items i
               INNER JOIN item_barcodes b ON i.id = b.item_id
               WHERE b.barcode = ?''',
            (barcode,)
        )
        return self._row_to_dict(row)
    
    def create(self, data: dict) -> str:
        item_id = generate_uuid()
        self.db.execute(
            '''INSERT INTO items (id, sku, name, name_en, description, category_id,
               uom_id, alt_uom_id, alt_uom_ratio, weight, volume, length, width, height,
               is_serialized, is_batch_tracked, is_expiry_tracked, min_expiry_days,
               storage_temp_min, storage_temp_max, storage_rules, is_active)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (item_id, data['sku'], data['name'], data.get('name_en'),
             data.get('description'), data.get('category_id'), data.get('uom_id'),
             data.get('alt_uom_id'), data.get('alt_uom_ratio', 1),
             data.get('weight', 0), data.get('volume', 0),
             data.get('length', 0), data.get('width', 0), data.get('height', 0),
             data.get('is_serialized', 0), data.get('is_batch_tracked', 0),
             data.get('is_expiry_tracked', 0), data.get('min_expiry_days', 0),
             data.get('storage_temp_min'), data.get('storage_temp_max'),
             data.get('storage_rules'), data.get('is_active', 1))
        )
        self.db.commit()
        return item_id
    
    def update(self, item_id: str, data: dict):
        allowed_fields = ['sku', 'name', 'name_en', 'description', 'category_id',
                         'uom_id', 'alt_uom_id', 'alt_uom_ratio', 'weight', 'volume',
                         'length', 'width', 'height', 'is_serialized', 'is_batch_tracked',
                         'is_expiry_tracked', 'min_expiry_days', 'storage_temp_min',
                         'storage_temp_max', 'storage_rules', 'is_active']
        fields = []
        values = []
        for key in allowed_fields:
            if key in data:
                fields.append(f'{key} = ?')
                values.append(data[key])
        
        if fields:
            fields.append('updated_at = ?')
            values.append(now_str())
            values.append(item_id)
            self.db.execute(
                f"UPDATE items SET {', '.join(fields)} WHERE id = ?",
                tuple(values)
            )
            self.db.commit()
    
    def delete(self, item_id: str):
        self.db.execute("DELETE FROM items WHERE id = ?", (item_id,))
        self.db.commit()
    
    def get_barcodes(self, item_id: str) -> List[dict]:
        return self._rows_to_dicts(
            self.db.fetchall(
                "SELECT * FROM item_barcodes WHERE item_id = ? ORDER BY is_primary DESC",
                (item_id,)
            )
        )
    
    def add_barcode(self, item_id: str, barcode: str, barcode_type: str = 'EAN13', 
                   is_primary: bool = False):
        self.db.execute(
            "INSERT INTO item_barcodes (id, item_id, barcode, barcode_type, is_primary) VALUES (?,?,?,?,?)",
            (generate_uuid(), item_id, barcode, barcode_type, 1 if is_primary else 0)
        )
        self.db.commit()
    
    def remove_barcode(self, barcode_id: str):
        self.db.execute("DELETE FROM item_barcodes WHERE id = ?", (barcode_id,))
        self.db.commit()


class ClientDAL(BaseDAL):
    """DAL для клієнтів 3PL"""
    
    def get_all(self, active_only: bool = True) -> List[dict]:
        sql = "SELECT * FROM clients"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY name"
        return self._rows_to_dicts(self.db.fetchall(sql))
    
    def get_by_id(self, client_id: str) -> Optional[dict]:
        row = self.db.fetchone("SELECT * FROM clients WHERE id = ?", (client_id,))
        return self._row_to_dict(row)
    
    def create(self, data: dict) -> str:
        client_id = generate_uuid()
        self.db.execute(
            '''INSERT INTO clients (id, code, name, legal_name, tax_id, address,
               phone, email, contract_number, contract_date, sla_days, tariff_type, is_active)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (client_id, data['code'], data['name'], data.get('legal_name'),
             data.get('tax_id'), data.get('address'), data.get('phone'),
             data.get('email'), data.get('contract_number'), data.get('contract_date'),
             data.get('sla_days', 3), data.get('tariff_type', 'operations'), 1)
        )
        self.db.commit()
        return client_id
    
    def update(self, client_id: str, data: dict):
        allowed_fields = ['code', 'name', 'legal_name', 'tax_id', 'address',
                         'phone', 'email', 'contract_number', 'contract_date',
                         'sla_days', 'tariff_type', 'is_active']
        fields = []
        values = []
        for key in allowed_fields:
            if key in data:
                fields.append(f'{key} = ?')
                values.append(data[key])
        
        if fields:
            fields.append('updated_at = ?')
            values.append(now_str())
            values.append(client_id)
            self.db.execute(
                f"UPDATE clients SET {', '.join(fields)} WHERE id = ?",
                tuple(values)
            )
            self.db.commit()
    
    def delete(self, client_id: str):
        self.db.execute("DELETE FROM clients WHERE id = ?", (client_id,))
        self.db.commit()


class SupplierDAL(BaseDAL):
    """DAL для постачальників"""
    
    def get_all(self, active_only: bool = True) -> List[dict]:
        sql = "SELECT * FROM suppliers"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY name"
        return self._rows_to_dicts(self.db.fetchall(sql))
    
    def get_by_id(self, supplier_id: str) -> Optional[dict]:
        row = self.db.fetchone("SELECT * FROM suppliers WHERE id = ?", (supplier_id,))
        return self._row_to_dict(row)
    
    def create(self, data: dict) -> str:
        supplier_id = generate_uuid()
        self.db.execute(
            '''INSERT INTO suppliers (id, code, name, legal_name, tax_id, address,
               phone, email, is_active) VALUES (?,?,?,?,?,?,?,?,?)''',
            (supplier_id, data['code'], data['name'], data.get('legal_name'),
             data.get('tax_id'), data.get('address'), data.get('phone'),
             data.get('email'), 1)
        )
        self.db.commit()
        return supplier_id
    
    def update(self, supplier_id: str, data: dict):
        allowed_fields = ['code', 'name', 'legal_name', 'tax_id', 'address',
                         'phone', 'email', 'is_active']
        fields = []
        values = []
        for key in allowed_fields:
            if key in data:
                fields.append(f'{key} = ?')
                values.append(data[key])
        
        if fields:
            fields.append('updated_at = ?')
            values.append(now_str())
            values.append(supplier_id)
            self.db.execute(
                f"UPDATE suppliers SET {', '.join(fields)} WHERE id = ?",
                tuple(values)
            )
            self.db.commit()
    
    def delete(self, supplier_id: str):
        self.db.execute("DELETE FROM suppliers WHERE id = ?", (supplier_id,))
        self.db.commit()


class WarehouseDAL(BaseDAL):
    """DAL для складів"""
    
    def get_all(self, active_only: bool = True) -> List[dict]:
        sql = "SELECT * FROM warehouses"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY code"
        return self._rows_to_dicts(self.db.fetchall(sql))
    
    def get_by_id(self, warehouse_id: str) -> Optional[dict]:
        row = self.db.fetchone("SELECT * FROM warehouses WHERE id = ?", (warehouse_id,))
        return self._row_to_dict(row)
    
    def create(self, data: dict) -> str:
        warehouse_id = generate_uuid()
        self.db.execute(
            "INSERT INTO warehouses (id, code, name, address, is_active) VALUES (?,?,?,?,?)",
            (warehouse_id, data['code'], data['name'], data.get('address'), 1)
        )
        self.db.commit()
        return warehouse_id
    
    def update(self, warehouse_id: str, data: dict):
        allowed_fields = ['code', 'name', 'address', 'is_active']
        fields = []
        values = []
        for key in allowed_fields:
            if key in data:
                fields.append(f'{key} = ?')
                values.append(data[key])
        
        if fields:
            fields.append('updated_at = ?')
            values.append(now_str())
            values.append(warehouse_id)
            self.db.execute(
                f"UPDATE warehouses SET {', '.join(fields)} WHERE id = ?",
                tuple(values)
            )
            self.db.commit()


class ZoneDAL(BaseDAL):
    """DAL для зон складу"""
    
    def get_all(self, warehouse_id: str = None) -> List[dict]:
        sql = '''SELECT z.*, w.name as warehouse_name 
                 FROM zones z 
                 LEFT JOIN warehouses w ON z.warehouse_id = w.id
                 WHERE z.is_active = 1'''
        params = []
        if warehouse_id:
            sql += ' AND z.warehouse_id = ?'
            params.append(warehouse_id)
        sql += ' ORDER BY z.code'
        return self._rows_to_dicts(self.db.fetchall(sql, tuple(params)))
    
    def get_by_id(self, zone_id: str) -> Optional[dict]:
        row = self.db.fetchone("SELECT * FROM zones WHERE id = ?", (zone_id,))
        return self._row_to_dict(row)
    
    def create(self, data: dict) -> str:
        zone_id = generate_uuid()
        self.db.execute(
            '''INSERT INTO zones (id, warehouse_id, code, name, zone_type, is_active) 
               VALUES (?,?,?,?,?,?)''',
            (zone_id, data['warehouse_id'], data['code'], data['name'],
             data['zone_type'], 1)
        )
        self.db.commit()
        return zone_id
    
    def update(self, zone_id: str, data: dict):
        allowed_fields = ['code', 'name', 'zone_type', 'is_active']
        fields = []
        values = []
        for key in allowed_fields:
            if key in data:
                fields.append(f'{key} = ?')
                values.append(data[key])
        
        if fields:
            fields.append('updated_at = ?')
            values.append(now_str())
            values.append(zone_id)
            self.db.execute(
                f"UPDATE zones SET {', '.join(fields)} WHERE id = ?",
                tuple(values)
            )
            self.db.commit()


class LocationDAL(BaseDAL):
    """DAL для комірок"""
    
    def get_all(self, zone_id: str = None, warehouse_id: str = None) -> List[dict]:
        sql = '''SELECT l.*, z.code as zone_code, z.name as zone_name, w.code as warehouse_code
                 FROM locations l 
                 INNER JOIN zones z ON l.zone_id = z.id
                 INNER JOIN warehouses w ON z.warehouse_id = w.id
                 WHERE l.is_active = 1'''
        params = []
        if zone_id:
            sql += ' AND l.zone_id = ?'
            params.append(zone_id)
        if warehouse_id:
            sql += ' AND z.warehouse_id = ?'
            params.append(warehouse_id)
        sql += ' ORDER BY l.code'
        return self._rows_to_dicts(self.db.fetchall(sql, tuple(params)))
    
    def get_by_id(self, location_id: str) -> Optional[dict]:
        row = self.db.fetchone("SELECT * FROM locations WHERE id = ?", (location_id,))
        return self._row_to_dict(row)
    
    def create(self, data: dict) -> str:
        location_id = generate_uuid()
        self.db.execute(
            '''INSERT INTO locations (id, zone_id, code, location_type, max_weight, 
               max_volume,                max_pallets, is_active) VALUES (?,?,?,?,?,?,?,?)''',
            (location_id, data['zone_id'], data['code'], data['location_type'],
             data.get('max_weight', 0), data.get('max_volume', 0),
             data.get('max_pallets', 1), 1)
        )
        self.db.commit()
        return location_id
    
    def update(self, location_id: str, data: dict):
        allowed_fields = ['code', 'location_type', 'max_weight', 'max_volume',
                         'max_pallets', 'is_active']
        fields = []
        values = []
        for key in allowed_fields:
            if key in data:
                fields.append(f'{key} = ?')
                values.append(data[key])
        
        if fields:
            fields.append('updated_at = ?')
            values.append(now_str())
            values.append(location_id)
            self.db.execute(
                f"UPDATE locations SET {', '.join(fields)} WHERE id = ?",
                tuple(values)
            )
            self.db.commit()


class InventoryDAL(BaseDAL):
    """DAL для залишків та рухів товарів"""
    
    def get_balances(self, client_id: str = None, warehouse_id: str = None,
                    item_id: str = None, location_id: str = None) -> List[dict]:
        sql = '''SELECT ib.*, i.sku, i.name as item_name, c.name as client_name,
                 w.name as warehouse_name, l.code as location_code
                 FROM inventory_balances ib
                 INNER JOIN items i ON ib.item_id = i.id
                 INNER JOIN clients c ON ib.client_id = c.id
                 INNER JOIN warehouses w ON ib.warehouse_id = w.id
                 LEFT JOIN locations l ON ib.location_id = l.id
                 WHERE (ib.qty_available > 0 OR ib.qty_reserved > 0 OR ib.qty_blocked > 0)'''
        params = []
        
        if client_id:
            sql += ' AND ib.client_id = ?'
            params.append(client_id)
        if warehouse_id:
            sql += ' AND ib.warehouse_id = ?'
            params.append(warehouse_id)
        if item_id:
            sql += ' AND ib.item_id = ?'
            params.append(item_id)
        if location_id:
            sql += ' AND ib.location_id = ?'
            params.append(location_id)
        
        sql += ' ORDER BY i.sku, l.code'
        return self._rows_to_dicts(self.db.fetchall(sql, tuple(params)))
    
    def get_balance(self, client_id: str, warehouse_id: str, item_id: str,
                   location_id: str = None, batch_number: str = None,
                   serial_number: str = None, expiry_date: str = None) -> Optional[dict]:
        sql = '''SELECT * FROM inventory_balances 
                 WHERE client_id = ? AND warehouse_id = ? AND item_id = ?'''
        params = [client_id, warehouse_id, item_id]
        
        if location_id:
            sql += ' AND location_id = ?'
            params.append(location_id)
        else:
            sql += ' AND location_id IS NULL'
        
        if batch_number:
            sql += ' AND batch_number = ?'
            params.append(batch_number)
        else:
            sql += ' AND batch_number IS NULL'
        
        if serial_number:
            sql += ' AND serial_number = ?'
            params.append(serial_number)
        else:
            sql += ' AND serial_number IS NULL'
        
        if expiry_date:
            sql += ' AND expiry_date = ?'
            params.append(expiry_date)
        else:
            sql += ' AND expiry_date IS NULL'
        
        row = self.db.fetchone(sql, tuple(params))
        return self._row_to_dict(row)
    
    def update_balance(self, client_id: str, warehouse_id: str, item_id: str,
                      qty_change: float, change_type: str = 'available',
                      location_id: str = None, batch_number: str = None,
                      serial_number: str = None, expiry_date: str = None):
        """Оновлення залишків"""
        balance = self.get_balance(client_id, warehouse_id, item_id, location_id,
                                   batch_number, serial_number, expiry_date)
        
        if balance:
            field_map = {
                'available': 'qty_available',
                'reserved': 'qty_reserved',
                'blocked': 'qty_blocked'
            }
            field = field_map.get(change_type, 'qty_available')
            new_qty = balance[field] + qty_change
            
            if new_qty < 0:
                raise ValueError(f"Недостатньо залишків. Наявно: {balance[field]}, потрібно: {abs(qty_change)}")
            
            self.db.execute(
                f"UPDATE inventory_balances SET {field} = ?, updated_at = ? WHERE id = ?",
                (new_qty, now_str(), balance['id'])
            )
        else:
            if qty_change < 0:
                raise ValueError("Неможливо зменшити залишки: товар відсутній на складі")
            
            balance_id = generate_uuid()
            qty_available = qty_change if change_type == 'available' else 0
            qty_reserved = qty_change if change_type == 'reserved' else 0
            qty_blocked = qty_change if change_type == 'blocked' else 0
            
            self.db.execute(
                '''INSERT INTO inventory_balances (id, client_id, warehouse_id, location_id,
                   item_id, batch_number, serial_number, expiry_date, qty_available,
                   qty_reserved, qty_blocked, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                (balance_id, client_id, warehouse_id, location_id, item_id,
                 batch_number, serial_number, expiry_date, qty_available,
                 qty_reserved, qty_blocked, now_str())
            )
        
        self.db.commit()
    
    def create_stock_move(self, move_type: str, doc_type: str, doc_id: str,
                         client_id: str, warehouse_id: str, item_id: str,
                         qty: float, user_id: str = None,
                         from_location_id: str = None, to_location_id: str = None,
                         batch_number: str = None, serial_number: str = None,
                         expiry_date: str = None, notes: str = None) -> str:
        move_id = generate_uuid()
        self.db.execute(
            '''INSERT INTO stock_moves (id, move_type, doc_type, doc_id, client_id,
               warehouse_id, from_location_id, to_location_id, item_id, batch_number,
               serial_number, expiry_date, qty, user_id, notes)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (move_id, move_type, doc_type, doc_id, client_id, warehouse_id,
             from_location_id, to_location_id, item_id, batch_number,
             serial_number, expiry_date, qty, user_id, notes)
        )
        self.db.commit()
        return move_id
    
    def get_stock_moves(self, client_id: str = None, item_id: str = None,
                       date_from: str = None, date_to: str = None,
                       move_type: str = None, limit: int = 1000) -> List[dict]:
        sql = '''SELECT sm.*, i.sku, i.name as item_name, c.name as client_name,
                 w.name as warehouse_name, u.username
                 FROM stock_moves sm
                 INNER JOIN items i ON sm.item_id = i.id
                 INNER JOIN clients c ON sm.client_id = c.id
                 INNER JOIN warehouses w ON sm.warehouse_id = w.id
                 LEFT JOIN users u ON sm.user_id = u.id
                 WHERE 1=1'''
        params = []
        
        if client_id:
            sql += ' AND sm.client_id = ?'
            params.append(client_id)
        if item_id:
            sql += ' AND sm.item_id = ?'
            params.append(item_id)
        if date_from:
            sql += ' AND DATE(sm.created_at) >= ?'
            params.append(date_from)
        if date_to:
            sql += ' AND DATE(sm.created_at) <= ?'
            params.append(date_to)
        if move_type:
            sql += ' AND sm.move_type = ?'
            params.append(move_type)
        
        sql += f' ORDER BY sm.created_at DESC LIMIT {limit}'
        return self._rows_to_dicts(self.db.fetchall(sql, tuple(params)))


class InboundOrderDAL(BaseDAL):
    """DAL для замовлень на приймання"""
    
    def get_all(self, client_id: str = None, status: str = None,
               date_from: str = None, date_to: str = None) -> List[dict]:
        sql = '''SELECT io.*, c.name as client_name, s.name as supplier_name,
                 w.name as warehouse_name, u.username as created_by_name
                 FROM inbound_orders io
                 INNER JOIN clients c ON io.client_id = c.id
                 LEFT JOIN suppliers s ON io.supplier_id = s.id
                 INNER JOIN warehouses w ON io.warehouse_id = w.id
                 LEFT JOIN users u ON io.created_by = u.id
                 WHERE 1=1'''
        params = []
        
        if client_id:
            sql += ' AND io.client_id = ?'
            params.append(client_id)
        if status:
            sql += ' AND io.status = ?'
            params.append(status)
        if date_from:
            sql += ' AND io.doc_date >= ?'
            params.append(date_from)
        if date_to:
            sql += ' AND io.doc_date <= ?'
            params.append(date_to)
        
        sql += ' ORDER BY io.doc_date DESC, io.doc_number DESC'
        return self._rows_to_dicts(self.db.fetchall(sql, tuple(params)))
    
    def get_by_id(self, order_id: str) -> Optional[dict]:
        row = self.db.fetchone(
            '''SELECT io.*, c.name as client_name, s.name as supplier_name,
               w.name as warehouse_name
               FROM inbound_orders io
               INNER JOIN clients c ON io.client_id = c.id
               LEFT JOIN suppliers s ON io.supplier_id = s.id
               INNER JOIN warehouses w ON io.warehouse_id = w.id
               WHERE io.id = ?''',
            (order_id,)
        )
        return self._row_to_dict(row)
    
    def get_lines(self, order_id: str) -> List[dict]:
        return self._rows_to_dicts(self.db.fetchall(
            '''SELECT iol.*, i.sku, i.name as item_name
               FROM inbound_order_lines iol
               INNER JOIN items i ON iol.item_id = i.id
               WHERE iol.order_id = ?
               ORDER BY iol.line_number''',
            (order_id,)
        ))
    
    def create(self, data: dict, lines: List[dict], user_id: str) -> str:
        order_id = generate_uuid()
        doc_number = generate_doc_number('IN')
        
        with self.db.transaction():
            self.db.execute(
                '''INSERT INTO inbound_orders (id, doc_number, doc_date, status, client_id,
                   supplier_id, warehouse_id, expected_date, notes, created_by)
                   VALUES (?,?,?,?,?,?,?,?,?,?)''',
                (order_id, doc_number, data.get('doc_date', today_str()),
                 DocStatus.DRAFT.value, data['client_id'], data.get('supplier_id'),
                 data['warehouse_id'], data.get('expected_date'), data.get('notes'), user_id)
            )
            
            for idx, line in enumerate(lines, 1):
                self.db.execute(
                    '''INSERT INTO inbound_order_lines (id, order_id, line_number, item_id,
                       expected_qty, batch_number, expiry_date, notes)
                       VALUES (?,?,?,?,?,?,?,?)''',
                    (generate_uuid(), order_id, idx, line['item_id'],
                     line['expected_qty'], line.get('batch_number'),
                     line.get('expiry_date'), line.get('notes'))
                )
        
        return order_id
    
    def update(self, order_id: str, data: dict, lines: List[dict] = None):
        order = self.get_by_id(order_id)
        if not order:
            raise ValueError("Замовлення не знайдено")
        if order['status'] == DocStatus.POSTED.value:
            raise ValueError("Неможливо редагувати проведений документ")
        
        with self.db.transaction():
            allowed_fields = ['doc_date', 'client_id', 'supplier_id', 'warehouse_id',
                            'expected_date', 'notes']
            fields = []
            values = []
            for key in allowed_fields:
                if key in data:
                    fields.append(f'{key} = ?')
                    values.append(data[key])
            
            if fields:
                fields.append('updated_at = ?')
                values.append(now_str())
                values.append(order_id)
                self.db.execute(
                    f"UPDATE inbound_orders SET {', '.join(fields)} WHERE id = ?",
                    tuple(values)
                )
            
            if lines is not None:
                self.db.execute("DELETE FROM inbound_order_lines WHERE order_id = ?", (order_id,))
                for idx, line in enumerate(lines, 1):
                    self.db.execute(
                        '''INSERT INTO inbound_order_lines (id, order_id, line_number, item_id,
                           expected_qty, received_qty, batch_number, expiry_date, notes)
                           VALUES (?,?,?,?,?,?,?,?,?)''',
                        (generate_uuid(), order_id, idx, line['item_id'],
                         line['expected_qty'], line.get('received_qty', 0),
                         line.get('batch_number'), line.get('expiry_date'), line.get('notes'))
                    )
    
    def update_status(self, order_id: str, status: str, user_id: str = None):
        with self.db.transaction():
            update_fields = ['status = ?', 'updated_at = ?']
            values = [status, now_str()]
            
            if status == DocStatus.POSTED.value:
                update_fields.extend(['posted_at = ?', 'posted_by = ?'])
                values.extend([now_str(), user_id])
            
            values.append(order_id)
            self.db.execute(
                f"UPDATE inbound_orders SET {', '.join(update_fields)} WHERE id = ?",
                tuple(values)
            )
    
    def update_line_received(self, line_id: str, received_qty: float):
        self.db.execute(
            "UPDATE inbound_order_lines SET received_qty = ? WHERE id = ?",
            (received_qty, line_id)
        )
        self.db.commit()
    
    def delete(self, order_id: str):
        order = self.get_by_id(order_id)
        if order and order['status'] == DocStatus.POSTED.value:
            raise ValueError("Неможливо видалити проведений документ")
        self.db.execute("DELETE FROM inbound_orders WHERE id = ?", (order_id,))
        self.db.commit()


class OutboundOrderDAL(BaseDAL):
    """DAL для замовлень на відвантаження"""
    
    def get_all(self, client_id: str = None, status: str = None,
               date_from: str = None, date_to: str = None) -> List[dict]:
        sql = '''SELECT oo.*, c.name as client_name, w.name as warehouse_name,
                 cr.name as carrier_name, u.username as created_by_name
                 FROM outbound_orders oo
                 INNER JOIN clients c ON oo.client_id = c.id
                 INNER JOIN warehouses w ON oo.warehouse_id = w.id
                 LEFT JOIN carriers cr ON oo.carrier_id = cr.id
                 LEFT JOIN users u ON oo.created_by = u.id
                 WHERE 1=1'''
        params = []
        
        if client_id:
            sql += ' AND oo.client_id = ?'
            params.append(client_id)
        if status:
            sql += ' AND oo.status = ?'
            params.append(status)
        if date_from:
            sql += ' AND oo.doc_date >= ?'
            params.append(date_from)
        if date_to:
            sql += ' AND oo.doc_date <= ?'
            params.append(date_to)
        
        sql += ' ORDER BY oo.doc_date DESC, oo.doc_number DESC'
        return self._rows_to_dicts(self.db.fetchall(sql, tuple(params)))
    
    def get_by_id(self, order_id: str) -> Optional[dict]:
        row = self.db.fetchone(
            '''SELECT oo.*, c.name as client_name, w.name as warehouse_name,
               cr.name as carrier_name
               FROM outbound_orders oo
               INNER JOIN clients c ON oo.client_id = c.id
               INNER JOIN warehouses w ON oo.warehouse_id = w.id
               LEFT JOIN carriers cr ON oo.carrier_id = cr.id
               WHERE oo.id = ?''',
            (order_id,)
        )
        return self._row_to_dict(row)
    
    def get_lines(self, order_id: str) -> List[dict]:
        return self._rows_to_dicts(self.db.fetchall(
            '''SELECT ool.*, i.sku, i.name as item_name
               FROM outbound_order_lines ool
               INNER JOIN items i ON ool.item_id = i.id
               WHERE ool.order_id = ?
               ORDER BY ool.line_number''',
            (order_id,)
        ))
    
    def create(self, data: dict, lines: List[dict], user_id: str) -> str:
        order_id = generate_uuid()
        doc_number = generate_doc_number('OUT')
        
        with self.db.transaction():
            self.db.execute(
                '''INSERT INTO outbound_orders (id, doc_number, doc_date, status, client_id,
                   warehouse_id, carrier_id, delivery_address, delivery_date, priority,
                   notes, created_by)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
                (order_id, doc_number, data.get('doc_date', today_str()),
                 DocStatus.DRAFT.value, data['client_id'], data['warehouse_id'],
                 data.get('carrier_id'), data.get('delivery_address'),
                 data.get('delivery_date'), data.get('priority', 5),
                 data.get('notes'), user_id)
            )
            
            for idx, line in enumerate(lines, 1):
                self.db.execute(
                    '''INSERT INTO outbound_order_lines (id, order_id, line_number, item_id,
                       ordered_qty, batch_number, expiry_date, notes)
                       VALUES (?,?,?,?,?,?,?,?)''',
                    (generate_uuid(), order_id, idx, line['item_id'],
                     line['ordered_qty'], line.get('batch_number'),
                     line.get('expiry_date'), line.get('notes'))
                )
        
        return order_id
    
    def update(self, order_id: str, data: dict, lines: List[dict] = None):
        order = self.get_by_id(order_id)
        if not order:
            raise ValueError("Замовлення не знайдено")
        if order['status'] == DocStatus.POSTED.value:
            raise ValueError("Неможливо редагувати проведений документ")
        
        with self.db.transaction():
            allowed_fields = ['doc_date', 'client_id', 'warehouse_id', 'carrier_id',
                            'delivery_address', 'delivery_date', 'priority', 'notes']
            fields = []
            values = []
            for key in allowed_fields:
                if key in data:
                    fields.append(f'{key} = ?')
                    values.append(data[key])
            
            if fields:
                fields.append('updated_at = ?')
                values.append(now_str())
                values.append(order_id)
                self.db.execute(
                    f"UPDATE outbound_orders SET {', '.join(fields)} WHERE id = ?",
                    tuple(values)
                )
            
            if lines is not None:
                self.db.execute("DELETE FROM outbound_order_lines WHERE order_id = ?", (order_id,))
                for idx, line in enumerate(lines, 1):
                    self.db.execute(
                        '''INSERT INTO outbound_order_lines (id, order_id, line_number, item_id,
                           ordered_qty, reserved_qty, picked_qty, shipped_qty, batch_number,
                           expiry_date, notes)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                        (generate_uuid(), order_id, idx, line['item_id'],
                         line['ordered_qty'], line.get('reserved_qty', 0),
                         line.get('picked_qty', 0), line.get('shipped_qty', 0),
                         line.get('batch_number'), line.get('expiry_date'), line.get('notes'))
                    )
    
    def update_status(self, order_id: str, status: str, user_id: str = None):
        with self.db.transaction():
            update_fields = ['status = ?', 'updated_at = ?']
            values = [status, now_str()]
            
            if status == DocStatus.POSTED.value:
                update_fields.extend(['posted_at = ?', 'posted_by = ?'])
                values.extend([now_str(), user_id])
            elif status == 'Відвантажено':
                update_fields.append('shipped_at = ?')
                values.append(now_str())
            
            values.append(order_id)
            self.db.execute(
                f"UPDATE outbound_orders SET {', '.join(update_fields)} WHERE id = ?",
                tuple(values)
            )
    
    def delete(self, order_id: str):
        order = self.get_by_id(order_id)
        if order and order['status'] == DocStatus.POSTED.value:
            raise ValueError("Неможливо видалити проведений документ")
        self.db.execute("DELETE FROM outbound_orders WHERE id = ?", (order_id,))
        self.db.commit()


class InventoryCountDAL(BaseDAL):
    """DAL для інвентаризацій"""
    
    def get_all(self, warehouse_id: str = None, status: str = None) -> List[dict]:
        sql = '''SELECT ic.*, w.name as warehouse_name, c.name as client_name,
                 z.name as zone_name
                 FROM inventory_counts ic
                 INNER JOIN warehouses w ON ic.warehouse_id = w.id
                 LEFT JOIN clients c ON ic.client_id = c.id
                 LEFT JOIN zones z ON ic.zone_id = z.id
                 WHERE 1=1'''
        params = []
        
        if warehouse_id:
            sql += ' AND ic.warehouse_id = ?'
            params.append(warehouse_id)
        if status:
            sql += ' AND ic.status = ?'
            params.append(status)
        
        sql += ' ORDER BY ic.doc_date DESC'
        return self._rows_to_dicts(self.db.fetchall(sql, tuple(params)))
    
    def get_by_id(self, count_id: str) -> Optional[dict]:
        row = self.db.fetchone(
            '''SELECT ic.*, w.name as warehouse_name, c.name as client_name
               FROM inventory_counts ic
               INNER JOIN warehouses w ON ic.warehouse_id = w.id
               LEFT JOIN clients c ON ic.client_id = c.id
               WHERE ic.id = ?''',
            (count_id,)
        )
        return self._row_to_dict(row)
    
    def get_lines(self, count_id: str) -> List[dict]:
        return self._rows_to_dicts(self.db.fetchall(
            '''SELECT icl.*, i.sku, i.name as item_name, l.code as location_code
               FROM inventory_count_lines icl
               INNER JOIN items i ON icl.item_id = i.id
               LEFT JOIN locations l ON icl.location_id = l.id
               WHERE icl.count_id = ?
               ORDER BY i.sku''',
            (count_id,)
        ))
    
    def create(self, data: dict, user_id: str) -> str:
        count_id = generate_uuid()
        doc_number = generate_doc_number('INV')
        
        self.db.execute(
            '''INSERT INTO inventory_counts (id, doc_number, doc_date, status, client_id,
               warehouse_id, zone_id, count_type, notes, created_by)
               VALUES (?,?,?,?,?,?,?,?,?,?)''',
            (count_id, doc_number, data.get('doc_date', today_str()),
             DocStatus.DRAFT.value, data.get('client_id'), data['warehouse_id'],
             data.get('zone_id'), data.get('count_type', 'full'),
             data.get('notes'), user_id)
        )
        self.db.commit()
        return count_id
    
    def add_line(self, count_id: str, data: dict):
        self.db.execute(
            '''INSERT INTO inventory_count_lines (id, count_id, location_id, item_id,
               batch_number, expiry_date, system_qty, counted_qty, difference, notes)
               VALUES (?,?,?,?,?,?,?,?,?,?)''',
            (generate_uuid(), count_id, data.get('location_id'), data['item_id'],
             data.get('batch_number'), data.get('expiry_date'),
             data.get('system_qty', 0), data.get('counted_qty', 0),
             data.get('counted_qty', 0) - data.get('system_qty', 0),
             data.get('notes'))
        )
        self.db.commit()
    
    def update_line(self, line_id: str, counted_qty: float):
        line = self.db.fetchone("SELECT system_qty FROM inventory_count_lines WHERE id = ?", (line_id,))
        if line:
            diff = counted_qty - line['system_qty']
            self.db.execute(
                "UPDATE inventory_count_lines SET counted_qty = ?, difference = ? WHERE id = ?",
                (counted_qty, diff, line_id)
            )
            self.db.commit()
    
    def update_status(self, count_id: str, status: str, user_id: str = None):
        with self.db.transaction():
            update_fields = ['status = ?', 'updated_at = ?']
            values = [status, now_str()]
            
            if status == DocStatus.POSTED.value:
                update_fields.extend(['posted_at = ?', 'posted_by = ?'])
                values.extend([now_str(), user_id])
            
            values.append(count_id)
            self.db.execute(
                f"UPDATE inventory_counts SET {', '.join(update_fields)} WHERE id = ?",
                tuple(values)
            )
    
    def delete(self, count_id: str):
        count = self.get_by_id(count_id)
        if count and count['status'] == DocStatus.POSTED.value:
            raise ValueError("Неможливо видалити проведений документ")
        self.db.execute("DELETE FROM inventory_counts WHERE id = ?", (count_id,))
        self.db.commit()


class WriteoffDAL(BaseDAL):
    """DAL для списань"""
    
    def get_all(self, client_id: str = None, status: str = None) -> List[dict]:
        sql = '''SELECT wo.*, c.name as client_name, w.name as warehouse_name
                 FROM writeoffs wo
                 INNER JOIN clients c ON wo.client_id = c.id
                 INNER JOIN warehouses w ON wo.warehouse_id = w.id
                 WHERE 1=1'''
        params = []
        
        if client_id:
            sql += ' AND wo.client_id = ?'
            params.append(client_id)
        if status:
            sql += ' AND wo.status = ?'
            params.append(status)
        
        sql += ' ORDER BY wo.doc_date DESC'
        return self._rows_to_dicts(self.db.fetchall(sql, tuple(params)))
    
    def get_by_id(self, writeoff_id: str) -> Optional[dict]:
        row = self.db.fetchone(
            '''SELECT wo.*, c.name as client_name, w.name as warehouse_name
               FROM writeoffs wo
               INNER JOIN clients c ON wo.client_id = c.id
               INNER JOIN warehouses w ON wo.warehouse_id = w.id
               WHERE wo.id = ?''',
            (writeoff_id,)
        )
        return self._row_to_dict(row)
    
    def get_lines(self, writeoff_id: str) -> List[dict]:
        return self._rows_to_dicts(self.db.fetchall(
            '''SELECT wol.*, i.sku, i.name as item_name, l.code as location_code
               FROM writeoff_lines wol
               INNER JOIN items i ON wol.item_id = i.id
               LEFT JOIN locations l ON wol.location_id = l.id
               WHERE wol.writeoff_id = ?''',
            (writeoff_id,)
        ))
    
    def create(self, data: dict, lines: List[dict], user_id: str) -> str:
        writeoff_id = generate_uuid()
        doc_number = generate_doc_number('WO')
        
        with self.db.transaction():
            self.db.execute(
                '''INSERT INTO writeoffs (id, doc_number, doc_date, status, client_id,
                   warehouse_id, reason, notes, created_by)
                   VALUES (?,?,?,?,?,?,?,?,?)''',
                (writeoff_id, doc_number, data.get('doc_date', today_str()),
                 DocStatus.DRAFT.value, data['client_id'], data['warehouse_id'],
                 data.get('reason'), data.get('notes'), user_id)
            )
            
            for line in lines:
                self.db.execute(
                    '''INSERT INTO writeoff_lines (id, writeoff_id, location_id, item_id,
                       qty, batch_number, reason, notes)
                       VALUES (?,?,?,?,?,?,?,?)''',
                    (generate_uuid(), writeoff_id, line.get('location_id'),
                     line['item_id'], line['qty'], line.get('batch_number'),
                     line.get('reason'), line.get('notes'))
                )
        
        return writeoff_id
    
    def update_status(self, writeoff_id: str, status: str, user_id: str = None):
        with self.db.transaction():
            update_fields = ['status = ?', 'updated_at = ?']
            values = [status, now_str()]
            
            if status == DocStatus.POSTED.value:
                update_fields.extend(['posted_at = ?', 'posted_by = ?'])
                values.extend([now_str(), user_id])
            
            values.append(writeoff_id)
            self.db.execute(
                f"UPDATE writeoffs SET {', '.join(update_fields)} WHERE id = ?",
                tuple(values)
            )
    
    def delete(self, writeoff_id: str):
        wo = self.get_by_id(writeoff_id)
        if wo and wo['status'] == DocStatus.POSTED.value:
            raise ValueError("Неможливо видалити проведений документ")
        self.db.execute("DELETE FROM writeoffs WHERE id = ?", (writeoff_id,))
        self.db.commit()


class ReturnDAL(BaseDAL):
    """DAL для повернень"""
    
    def get_all(self, client_id: str = None, status: str = None) -> List[dict]:
        sql = '''SELECT r.*, c.name as client_name, w.name as warehouse_name
                 FROM returns r
                 INNER JOIN clients c ON r.client_id = c.id
                 INNER JOIN warehouses w ON r.warehouse_id = w.id
                 WHERE 1=1'''
        params = []
        
        if client_id:
            sql += ' AND r.client_id = ?'
            params.append(client_id)
        if status:
            sql += ' AND r.status = ?'
            params.append(status)
        
        sql += ' ORDER BY r.doc_date DESC'
        return self._rows_to_dicts(self.db.fetchall(sql, tuple(params)))
    
    def get_by_id(self, return_id: str) -> Optional[dict]:
        row = self.db.fetchone(
            '''SELECT r.*, c.name as client_name, w.name as warehouse_name
               FROM returns r
               INNER JOIN clients c ON r.client_id = c.id
               INNER JOIN warehouses w ON r.warehouse_id = w.id
               WHERE r.id = ?''',
            (return_id,)
        )
        return self._row_to_dict(row)
    
    def get_lines(self, return_id: str) -> List[dict]:
        return self._rows_to_dicts(self.db.fetchall(
            '''SELECT rl.*, i.sku, i.name as item_name
               FROM return_lines rl
               INNER JOIN items i ON rl.item_id = i.id
               WHERE rl.return_id = ?''',
            (return_id,)
        ))
    
    def create(self, data: dict, lines: List[dict], user_id: str) -> str:
        return_id = generate_uuid()
        doc_number = generate_doc_number('RET')
        
        with self.db.transaction():
            self.db.execute(
                '''INSERT INTO returns (id, doc_number, doc_date, status, return_type,
                   client_id, warehouse_id, original_order_id, notes, created_by)
                   VALUES (?,?,?,?,?,?,?,?,?,?)''',
                (return_id, doc_number, data.get('doc_date', today_str()),
                 DocStatus.DRAFT.value, data.get('return_type', 'customer'),
                 data['client_id'], data['warehouse_id'],
                 data.get('original_order_id'), data.get('notes'), user_id)
            )
            
            for line in lines:
                self.db.execute(
                    '''INSERT INTO return_lines (id, return_id, item_id, qty, condition,
                       destination_zone, batch_number, notes)
                       VALUES (?,?,?,?,?,?,?,?)''',
                    (generate_uuid(), return_id, line['item_id'], line['qty'],
                     line.get('condition', 'good'), line.get('destination_zone', 'STORAGE'),
                     line.get('batch_number'), line.get('notes'))
                )
        
        return return_id
    
    def update_status(self, return_id: str, status: str, user_id: str = None):
        with self.db.transaction():
            update_fields = ['status = ?', 'updated_at = ?']
            values = [status, now_str()]
            
            if status == DocStatus.POSTED.value:
                update_fields.extend(['posted_at = ?', 'posted_by = ?'])
                values.extend([now_str(), user_id])
            
            values.append(return_id)
            self.db.execute(
                f"UPDATE returns SET {', '.join(update_fields)} WHERE id = ?",
                tuple(values)
            )
    
    def delete(self, return_id: str):
        ret = self.get_by_id(return_id)
        if ret and ret['status'] == DocStatus.POSTED.value:
            raise ValueError("Неможливо видалити проведений документ")
        self.db.execute("DELETE FROM returns WHERE id = ?", (return_id,))
        self.db.commit()


class AuditDAL(BaseDAL):
    """DAL для журналу аудиту"""
    
    def log(self, user_id: str, username: str, action: str,
           entity_type: str = None, entity_id: str = None,
           old_values: dict = None, new_values: dict = None):
        self.db.execute(
            '''INSERT INTO audit_log (id, user_id, username, action, entity_type,
               entity_id, old_values, new_values)
               VALUES (?,?,?,?,?,?,?,?)''',
            (generate_uuid(), user_id, username, action, entity_type, entity_id,
             json.dumps(old_values, ensure_ascii=False) if old_values else None,
             json.dumps(new_values, ensure_ascii=False) if new_values else None)
        )
        self.db.commit()
    
    def get_logs(self, user_id: str = None, entity_type: str = None,
                entity_id: str = None, date_from: str = None,
                date_to: str = None, limit: int = 500) -> List[dict]:
        sql = "SELECT * FROM audit_log WHERE 1=1"
        params = []
        
        if user_id:
            sql += ' AND user_id = ?'
            params.append(user_id)
        if entity_type:
            sql += ' AND entity_type = ?'
            params.append(entity_type)
        if entity_id:
            sql += ' AND entity_id = ?'
            params.append(entity_id)
        if date_from:
            sql += ' AND DATE(created_at) >= ?'
            params.append(date_from)
        if date_to:
            sql += ' AND DATE(created_at) <= ?'
            params.append(date_to)
        
        sql += f' ORDER BY created_at DESC LIMIT {limit}'
        return self._rows_to_dicts(self.db.fetchall(sql, tuple(params)))


class CategoryDAL(BaseDAL):
    """DAL для категорій товарів"""
    
    def get_all(self) -> List[dict]:
        return self._rows_to_dicts(
            self.db.fetchall("SELECT * FROM item_categories ORDER BY name")
        )
    
    def get_by_id(self, category_id: str) -> Optional[dict]:
        row = self.db.fetchone("SELECT * FROM item_categories WHERE id = ?", (category_id,))
        return self._row_to_dict(row)


class UomDAL(BaseDAL):
    """DAL для одиниць виміру"""
    
    def get_all(self) -> List[dict]:
        return self._rows_to_dicts(
            self.db.fetchall("SELECT * FROM uom ORDER BY name")
        )
    
    def get_by_id(self, uom_id: str) -> Optional[dict]:
        row = self.db.fetchone("SELECT * FROM uom WHERE id = ?", (uom_id,))
        return self._row_to_dict(row)


class CarrierDAL(BaseDAL):
    """DAL для перевізників"""
    
    def get_all(self, active_only: bool = True) -> List[dict]:
        sql = "SELECT * FROM carriers"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY name"
        return self._rows_to_dicts(self.db.fetchall(sql))
    
    def get_by_id(self, carrier_id: str) -> Optional[dict]:
        row = self.db.fetchone("SELECT * FROM carriers WHERE id = ?", (carrier_id,))
        return self._row_to_dict(row)
    
    def create(self, data: dict) -> str:
        carrier_id = generate_uuid()
        self.db.execute(
            "INSERT INTO carriers (id, code, name, phone, email, is_active) VALUES (?,?,?,?,?,?)",
            (carrier_id, data['code'], data['name'], data.get('phone'),
             data.get('email'), 1)
        )
        self.db.commit()
        return carrier_id
    
    def update(self, carrier_id: str, data: dict):
        allowed_fields = ['code', 'name', 'phone', 'email', 'is_active']
        fields = []
        values = []
        for key in allowed_fields:
            if key in data:
                fields.append(f'{key} = ?')
                values.append(data[key])
        
        if fields:
            fields.append('updated_at = ?')
            values.append(now_str())
            values.append(carrier_id)
            self.db.execute(
                f"UPDATE carriers SET {', '.join(fields)} WHERE id = ?",
                tuple(values)
            )
            self.db.commit()
    
    def delete(self, carrier_id: str):
        self.db.execute("DELETE FROM carriers WHERE id = ?", (carrier_id,))
        self.db.commit()


# ============================================================================
# SERVICES (Бізнес-логіка)
# ============================================================================

class AuthService:
    """Сервіс аутентифікації"""
    
    def __init__(self):
        self.user_dal = UserDAL()
        self.audit_dal = AuditDAL()
        self.current_user = None
        self.permissions = []
    
    def login(self, username: str, password: str) -> Tuple[bool, str]:
        user = self.user_dal.get_by_username(username)
        
        if not user:
            return False, "Користувача не знайдено"
        
        if not user['is_active']:
            return False, "Користувач деактивований"
        
        if user['locked_until']:
            locked_until = datetime.strptime(user['locked_until'], '%Y-%m-%d %H:%M:%S')
            if datetime.now() < locked_until:
                return False, f"Обліковий запис заблоковано до {user['locked_until']}"
        
        if not verify_password(password, user['password_hash'], user['password_salt']):
            self.user_dal.update_login_attempt(user['id'], False)
            attempts_left = 5 - user['failed_attempts'] - 1
            if attempts_left > 0:
                return False, f"Невірний пароль. Залишилось спроб: {attempts_left}"
            else:
                return False, "Обліковий запис заблоковано на 30 хвилин"
        
        self.user_dal.update_login_attempt(user['id'], True)
        self.current_user = user
        self.permissions = self.user_dal.get_permissions(user['id'])
        
        self.audit_dal.log(user['id'], username, 'LOGIN')
        logger.info(f"User {username} logged in successfully")
        
        return True, "Успішний вхід"
    
    def logout(self):
        if self.current_user:
            self.audit_dal.log(
                self.current_user['id'],
                self.current_user['username'],
                'LOGOUT'
            )
            logger.info(f"User {self.current_user['username']} logged out")
        self.current_user = None
        self.permissions = []
    
    def has_permission(self, permission: str) -> bool:
        if not self.current_user:
            return False
        if self.current_user['role_name'] == 'Адміністратор':
            return True
        return permission in self.permissions
    
    def check_permission(self, permission: str) -> bool:
        if not self.has_permission(permission):
            raise PermissionError(f"Недостатньо прав для виконання операції: {permission}")
        return True


class InventoryService:
    """Сервіс управління запасами"""
    
    def __init__(self, auth_service: AuthService):
        self.auth = auth_service
        self.inventory_dal = InventoryDAL()
        self.audit_dal = AuditDAL()
    
    def receive_stock(self, client_id: str, warehouse_id: str, item_id: str,
                     qty: float, doc_type: str, doc_id: str,
                     location_id: str = None, batch_number: str = None,
                     serial_number: str = None, expiry_date: str = None):
        """Прихід товару на склад"""
        with DatabaseManager().transaction():
            # Створюємо рух
            self.inventory_dal.create_stock_move(
                MoveType.IN_RECEIPT.value, doc_type, doc_id, client_id,
                warehouse_id, item_id, qty,
                self.auth.current_user['id'] if self.auth.current_user else None,
                to_location_id=location_id, batch_number=batch_number,
                serial_number=serial_number, expiry_date=expiry_date
            )
            
            # Оновлюємо залишки
            self.inventory_dal.update_balance(
                client_id, warehouse_id, item_id, qty, 'available',
                location_id, batch_number, serial_number, expiry_date
            )
        
        logger.info(f"Stock received: item={item_id}, qty={qty}, doc={doc_id}")
    
    def ship_stock(self, client_id: str, warehouse_id: str, item_id: str,
                  qty: float, doc_type: str, doc_id: str,
                  location_id: str = None, batch_number: str = None,
                  serial_number: str = None, expiry_date: str = None):
        """Відвантаження товару зі складу"""
        with DatabaseManager().transaction():
            # Створюємо рух
            self.inventory_dal.create_stock_move(
                MoveType.SHIP.value, doc_type, doc_id, client_id,
                warehouse_id, item_id, -qty,
                self.auth.current_user['id'] if self.auth.current_user else None,
                from_location_id=location_id, batch_number=batch_number,
                serial_number=serial_number, expiry_date=expiry_date
            )
            
            # Оновлюємо залишки
            self.inventory_dal.update_balance(
                client_id, warehouse_id, item_id, -qty, 'available',
                location_id, batch_number, serial_number, expiry_date
            )
        
        logger.info(f"Stock shipped: item={item_id}, qty={qty}, doc={doc_id}")
    
    def reserve_stock(self, client_id: str, warehouse_id: str, item_id: str,
                     qty: float, doc_type: str, doc_id: str,
                     location_id: str = None, batch_number: str = None):
        """Резервування товару"""
        with DatabaseManager().transaction():
            # Зменшуємо доступне, збільшуємо резерв
            self.inventory_dal.update_balance(
                client_id, warehouse_id, item_id, -qty, 'available',
                location_id, batch_number
            )
            self.inventory_dal.update_balance(
                client_id, warehouse_id, item_id, qty, 'reserved',
                location_id, batch_number
            )
            
            # Створюємо рух
            self.inventory_dal.create_stock_move(
                MoveType.RESERVE.value, doc_type, doc_id, client_id,
                warehouse_id, item_id, qty,
                self.auth.current_user['id'] if self.auth.current_user else None
            )
        
        logger.info(f"Stock reserved: item={item_id}, qty={qty}")
    
    def unreserve_stock(self, client_id: str, warehouse_id: str, item_id: str,
                       qty: float, doc_type: str, doc_id: str,
                       location_id: str = None, batch_number: str = None):
        """Зняття резерву"""
        with DatabaseManager().transaction():
            # Зменшуємо резерв, збільшуємо доступне
            self.inventory_dal.update_balance(
                client_id, warehouse_id, item_id, -qty, 'reserved',
                location_id, batch_number
            )
            self.inventory_dal.update_balance(
                client_id, warehouse_id, item_id, qty, 'available',
                location_id, batch_number
            )
            
            # Створюємо рух
            self.inventory_dal.create_stock_move(
                MoveType.UNRESERVE.value, doc_type, doc_id, client_id,
                warehouse_id, item_id, -qty,
                self.auth.current_user['id'] if self.auth.current_user else None
            )
        
        logger.info(f"Stock unreserved: item={item_id}, qty={qty}")
    
    def writeoff_stock(self, client_id: str, warehouse_id: str, item_id: str,
                      qty: float, doc_type: str, doc_id: str,
                      location_id: str = None, batch_number: str = None,
                      reason: str = None):
        """Списання товару"""
        with DatabaseManager().transaction():
            # Створюємо рух
            self.inventory_dal.create_stock_move(
                MoveType.WRITE_OFF.value, doc_type, doc_id, client_id,
                warehouse_id, item_id, -qty,
                self.auth.current_user['id'] if self.auth.current_user else None,
                from_location_id=location_id, batch_number=batch_number,
                notes=reason
            )
            
            # Оновлюємо залишки
            self.inventory_dal.update_balance(
                client_id, warehouse_id, item_id, -qty, 'available',
                location_id, batch_number
            )
        
        logger.info(f"Stock written off: item={item_id}, qty={qty}, reason={reason}")
    
    def adjust_stock(self, client_id: str, warehouse_id: str, item_id: str,
                    qty_diff: float, doc_type: str, doc_id: str,
                    location_id: str = None, batch_number: str = None):
        """Коригування залишків (інвентаризація)"""
        with DatabaseManager().transaction():
            # Створюємо рух
            self.inventory_dal.create_stock_move(
                MoveType.ADJUSTMENT.value, doc_type, doc_id, client_id,
                warehouse_id, item_id, qty_diff,
                self.auth.current_user['id'] if self.auth.current_user else None,
                to_location_id=location_id if qty_diff > 0 else None,
                from_location_id=location_id if qty_diff < 0 else None,
                batch_number=batch_number
            )
            
            # Оновлюємо залишки
            self.inventory_dal.update_balance(
                client_id, warehouse_id, item_id, qty_diff, 'available',
                location_id, batch_number
            )
        
        logger.info(f"Stock adjusted: item={item_id}, diff={qty_diff}")
    
    def move_stock(self, client_id: str, warehouse_id: str, item_id: str,
                  qty: float, from_location_id: str, to_location_id: str,
                  batch_number: str = None):
        """Переміщення товару між комірками"""
        with DatabaseManager().transaction():
            # Зменшуємо на старій локації
            self.inventory_dal.update_balance(
                client_id, warehouse_id, item_id, -qty, 'available',
                from_location_id, batch_number
            )
            
            # Збільшуємо на новій локації
            self.inventory_dal.update_balance(
                client_id, warehouse_id, item_id, qty, 'available',
                to_location_id, batch_number
            )
            
            # Створюємо рух
            self.inventory_dal.create_stock_move(
                MoveType.MOVE.value, 'MOVE', None, client_id,
                warehouse_id, item_id, qty,
                self.auth.current_user['id'] if self.auth.current_user else None,
                from_location_id=from_location_id, to_location_id=to_location_id,
                batch_number=batch_number
            )
        
        logger.info(f"Stock moved: item={item_id}, qty={qty}, {from_location_id} -> {to_location_id}")


class InboundService:
    """Сервіс приймання товарів"""
    
    def __init__(self, auth_service: AuthService, inventory_service: InventoryService):
        self.auth = auth_service
        self.inventory = inventory_service
        self.inbound_dal = InboundOrderDAL()
        self.audit_dal = AuditDAL()
    
    def post_order(self, order_id: str):
        """Проведення замовлення на приймання"""
        self.auth.check_permission('inbound_post')
        
        order = self.inbound_dal.get_by_id(order_id)
        if not order:
            raise ValueError("Замовлення не знайдено")
        
        if order['status'] == DocStatus.POSTED.value:
            raise ValueError("Замовлення вже проведено")
        
        lines = self.inbound_dal.get_lines(order_id)
        if not lines:
            raise ValueError("Замовлення не містить позицій")
        
        # Перевіряємо що всі позиції прийняті
        for line in lines:
            if line['received_qty'] <= 0:
                raise ValueError(f"Позиція {line['sku']} не має прийнятої кількості")
        
        # Оприбутковуємо товари
        for line in lines:
            self.inventory.receive_stock(
                order['client_id'], order['warehouse_id'], line['item_id'],
                line['received_qty'], 'INBOUND', order_id,
                batch_number=line['batch_number'],
                expiry_date=line['expiry_date']
            )
        
        # Оновлюємо статус
        user_id = self.auth.current_user['id'] if self.auth.current_user else None
        self.inbound_dal.update_status(order_id, DocStatus.POSTED.value, user_id)
        
        # Аудит
        self.audit_dal.log(
            user_id,
            self.auth.current_user['username'] if self.auth.current_user else 'system',
            'POST_INBOUND',
            'inbound_orders', order_id
        )
        
        logger.info(f"Inbound order {order['doc_number']} posted")
    
    def cancel_posting(self, order_id: str):
        """Скасування проведення"""
        self.auth.check_permission('inbound_post')
        
        order = self.inbound_dal.get_by_id(order_id)
        if not order:
            raise ValueError("Замовлення не знайдено")
        
        if order['status'] != DocStatus.POSTED.value:
            raise ValueError("Замовлення не проведено")
        
        lines = self.inbound_dal.get_lines(order_id)
        
        # Списуємо товари назад
        for line in lines:
            if line['received_qty'] > 0:
                self.inventory.ship_stock(
                    order['client_id'], order['warehouse_id'], line['item_id'],
                    line['received_qty'], 'INBOUND_CANCEL', order_id,
                    batch_number=line['batch_number'],
                    expiry_date=line['expiry_date']
                )
        
        # Оновлюємо статус
        user_id = self.auth.current_user['id'] if self.auth.current_user else None
        self.inbound_dal.update_status(order_id, DocStatus.CANCELLED.value, user_id)
        
        logger.info(f"Inbound order {order['doc_number']} posting cancelled")


class OutboundService:
    """Сервіс відвантаження товарів"""
    
    def __init__(self, auth_service: AuthService, inventory_service: InventoryService):
        self.auth = auth_service
        self.inventory = inventory_service
        self.outbound_dal = OutboundOrderDAL()
        self.inventory_dal = InventoryDAL()
        self.audit_dal = AuditDAL()
    
    def reserve_order(self, order_id: str):
        """Резервування товарів під замовлення"""
        order = self.outbound_dal.get_by_id(order_id)
        if not order:
            raise ValueError("Замовлення не знайдено")
        
        lines = self.outbound_dal.get_lines(order_id)
        
        for line in lines:
            qty_to_reserve = line['ordered_qty'] - line['reserved_qty']
            if qty_to_reserve > 0:
                # Перевіряємо наявність
                balances = self.inventory_dal.get_balances(
                    client_id=order['client_id'],
                    warehouse_id=order['warehouse_id'],
                    item_id=line['item_id']
                )
                total_available = sum(b['qty_available'] for b in balances)
                
                if total_available < qty_to_reserve:
                    raise ValueError(
                        f"Недостатньо товару {line['sku']}. "
                        f"Потрібно: {qty_to_reserve}, наявно: {total_available}"
                    )
                
                # Резервуємо
                self.inventory.reserve_stock(
                    order['client_id'], order['warehouse_id'], line['item_id'],
                    qty_to_reserve, 'OUTBOUND', order_id
                )
        
        # Оновлюємо статус
        self.outbound_dal.update_status(order_id, DocStatus.IN_PROGRESS.value)
        logger.info(f"Outbound order {order['doc_number']} reserved")
    
    def post_order(self, order_id: str):
        """Проведення (відвантаження) замовлення"""
        self.auth.check_permission('outbound_post')
        
        order = self.outbound_dal.get_by_id(order_id)
        if not order:
            raise ValueError("Замовлення не знайдено")
        
        if order['status'] == DocStatus.POSTED.value:
            raise ValueError("Замовлення вже відвантажено")
        
        lines = self.outbound_dal.get_lines(order_id)
        
        for line in lines:
            qty_to_ship = line['ordered_qty']
            
            # Знімаємо резерв і відвантажуємо
            if line['reserved_qty'] > 0:
                self.inventory.unreserve_stock(
                    order['client_id'], order['warehouse_id'], line['item_id'],
                    line['reserved_qty'], 'OUTBOUND', order_id
                )
            
            self.inventory.ship_stock(
                order['client_id'], order['warehouse_id'], line['item_id'],
                qty_to_ship, 'OUTBOUND', order_id,
                batch_number=line['batch_number'],
                expiry_date=line['expiry_date']
            )
        
        # Оновлюємо статус
        user_id = self.auth.current_user['id'] if self.auth.current_user else None
        self.outbound_dal.update_status(order_id, DocStatus.POSTED.value, user_id)
        
        # Аудит
        self.audit_dal.log(
            user_id,
            self.auth.current_user['username'] if self.auth.current_user else 'system',
            'POST_OUTBOUND',
            'outbound_orders', order_id
        )
        
        logger.info(f"Outbound order {order['doc_number']} shipped")
    
    def cancel_posting(self, order_id: str):
        """Скасування відвантаження"""
        self.auth.check_permission('outbound_post')
        
        order = self.outbound_dal.get_by_id(order_id)
        if not order:
            raise ValueError("Замовлення не знайдено")
        
        if order['status'] != DocStatus.POSTED.value:
            raise ValueError("Замовлення не відвантажено")
        
        lines = self.outbound_dal.get_lines(order_id)
        
        # Повертаємо товари
        for line in lines:
            self.inventory.receive_stock(
                order['client_id'], order['warehouse_id'], line['item_id'],
                line['ordered_qty'], 'OUTBOUND_CANCEL', order_id,
                batch_number=line['batch_number'],
                expiry_date=line['expiry_date']
            )
        
        user_id = self.auth.current_user['id'] if self.auth.current_user else None
        self.outbound_dal.update_status(order_id, DocStatus.CANCELLED.value, user_id)
        
        logger.info(f"Outbound order {order['doc_number']} shipping cancelled")


class InventoryCountService:
    """Сервіс інвентаризації"""
    
    def __init__(self, auth_service: AuthService, inventory_service: InventoryService):
        self.auth = auth_service
        self.inventory = inventory_service
        self.count_dal = InventoryCountDAL()
        self.inventory_dal = InventoryDAL()
        self.audit_dal = AuditDAL()
    
    def fill_from_balances(self, count_id: str):
        """Заповнення інвентаризації з поточних залишків"""
        count = self.count_dal.get_by_id(count_id)
        if not count:
            raise ValueError("Інвентаризацію не знайдено")
        
        balances = self.inventory_dal.get_balances(
            client_id=count['client_id'],
            warehouse_id=count['warehouse_id']
        )
        
        for balance in balances:
            total_qty = balance['qty_available'] + balance['qty_reserved'] + balance['qty_blocked']
            if total_qty > 0:
                self.count_dal.add_line(count_id, {
                    'location_id': balance['location_id'],
                    'item_id': balance['item_id'],
                    'batch_number': balance['batch_number'],
                    'expiry_date': balance['expiry_date'],
                    'system_qty': total_qty,
                    'counted_qty': total_qty
                })
        
        logger.info(f"Inventory count {count['doc_number']} filled from balances")
    
    def post_count(self, count_id: str):
        """Проведення інвентаризації"""
        self.auth.check_permission('inventory_post')
        
        count = self.count_dal.get_by_id(count_id)
        if not count:
            raise ValueError("Інвентаризацію не знайдено")
        
        if count['status'] == DocStatus.POSTED.value:
            raise ValueError("Інвентаризація вже проведена")
        
        lines = self.count_dal.get_lines(count_id)
        
        # Визначаємо client_id
        client_id = count['client_id']
        if not client_id:
            # Якщо клієнт не вказаний, беремо першого
            from_balance = self.inventory_dal.get_balances(warehouse_id=count['warehouse_id'])
            if from_balance:
                client_id = from_balance[0]['client_id']
        
        if not client_id:
            raise ValueError("Не вдалося визначити клієнта для коригування")
        
        # Застосовуємо різниці
        for line in lines:
            if line['difference'] != 0:
                self.inventory.adjust_stock(
                    client_id, count['warehouse_id'], line['item_id'],
                    line['difference'], 'INVENTORY_COUNT', count_id,
                    location_id=line['location_id'],
                    batch_number=line['batch_number']
                )
        
        user_id = self.auth.current_user['id'] if self.auth.current_user else None
        self.count_dal.update_status(count_id, DocStatus.POSTED.value, user_id)
        
        self.audit_dal.log(
            user_id,
            self.auth.current_user['username'] if self.auth.current_user else 'system',
            'POST_INVENTORY_COUNT',
            'inventory_counts', count_id
        )
        
        logger.info(f"Inventory count {count['doc_number']} posted")


class WriteoffService:
    """Сервіс списань"""
    
    def __init__(self, auth_service: AuthService, inventory_service: InventoryService):
        self.auth = auth_service
        self.inventory = inventory_service
        self.writeoff_dal = WriteoffDAL()
        self.audit_dal = AuditDAL()
    
    def post_writeoff(self, writeoff_id: str):
        """Проведення списання"""
        self.auth.check_permission('writeoffs_post')
        
        writeoff = self.writeoff_dal.get_by_id(writeoff_id)
        if not writeoff:
            raise ValueError("Списання не знайдено")
        
        if writeoff['status'] == DocStatus.POSTED.value:
            raise ValueError("Списання вже проведено")
        
        lines = self.writeoff_dal.get_lines(writeoff_id)
        
        for line in lines:
            self.inventory.writeoff_stock(
                writeoff['client_id'], writeoff['warehouse_id'], line['item_id'],
                line['qty'], 'WRITEOFF', writeoff_id,
                location_id=line['location_id'],
                batch_number=line['batch_number'],
                reason=line['reason']
            )
        
        user_id = self.auth.current_user['id'] if self.auth.current_user else None
        self.writeoff_dal.update_status(writeoff_id, DocStatus.POSTED.value, user_id)
        
        self.audit_dal.log(
            user_id,
            self.auth.current_user['username'] if self.auth.current_user else 'system',
            'POST_WRITEOFF',
            'writeoffs', writeoff_id
        )
        
        logger.info(f"Writeoff {writeoff['doc_number']} posted")


class ReturnService:
    """Сервіс повернень"""
    
    def __init__(self, auth_service: AuthService, inventory_service: InventoryService):
        self.auth = auth_service
        self.inventory = inventory_service
        self.return_dal = ReturnDAL()
        self.audit_dal = AuditDAL()
    
    def post_return(self, return_id: str):
        """Проведення повернення"""
        self.auth.check_permission('returns_post')
        
        ret = self.return_dal.get_by_id(return_id)
        if not ret:
            raise ValueError("Повернення не знайдено")
        
        if ret['status'] == DocStatus.POSTED.value:
            raise ValueError("Повернення вже проведено")
        
        lines = self.return_dal.get_lines(return_id)
        
        for line in lines:
            # Оприбутковуємо повернення
            self.inventory.receive_stock(
                ret['client_id'], ret['warehouse_id'], line['item_id'],
                line['qty'], 'RETURN', return_id,
                batch_number=line['batch_number']
            )
        
        user_id = self.auth.current_user['id'] if self.auth.current_user else None
        self.return_dal.update_status(return_id, DocStatus.POSTED.value, user_id)
        
        self.audit_dal.log(
            user_id,
            self.auth.current_user['username'] if self.auth.current_user else 'system',
            'POST_RETURN',
            'returns', return_id
        )
        
        logger.info(f"Return {ret['doc_number']} posted")


class ReportService:
    """Сервіс звітів"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.inventory_dal = InventoryDAL()
    
    def get_stock_balance_report(self, client_id: str = None, warehouse_id: str = None,
                                 item_id: str = None) -> List[dict]:
        """Звіт по залишках"""
        return self.inventory_dal.get_balances(client_id, warehouse_id, item_id)
    
    def get_stock_moves_report(self, date_from: str = None, date_to: str = None,
                               client_id: str = None, item_id: str = None,
                               move_type: str = None) -> List[dict]:
        """Звіт по рухах товарів"""
        return self.inventory_dal.get_stock_moves(
            client_id, item_id, date_from, date_to, move_type
        )
    
    def get_turnover_report(self, date_from: str, date_to: str,
                           client_id: str = None) -> List[dict]:
        """Звіт по оборотності"""
        sql = '''
            SELECT 
                i.sku,
                i.name as item_name,
                c.name as client_name,
                SUM(CASE WHEN sm.qty > 0 THEN sm.qty ELSE 0 END) as qty_in,
                SUM(CASE WHEN sm.qty < 0 THEN ABS(sm.qty) ELSE 0 END) as qty_out,
                COUNT(*) as move_count
            FROM stock_moves sm
            INNER JOIN items i ON sm.item_id = i.id
            INNER JOIN clients c ON sm.client_id = c.id
            WHERE DATE(sm.created_at) BETWEEN ? AND ?
        '''
        params = [date_from, date_to]
        
        if client_id:
            sql += ' AND sm.client_id = ?'
            params.append(client_id)
        
        sql += ' GROUP BY i.id, c.id ORDER BY qty_out DESC'
        
        rows = self.db.fetchall(sql, tuple(params))
        return [dict(row) for row in rows]
    
    def get_expiry_report(self, days_ahead: int = 30,
                         client_id: str = None) -> List[dict]:
        """Звіт по термінам придатності"""
        future_date = (datetime.now() + timedelta(days=days_ahead)).strftime('%Y-%m-%d')
        today = today_str()
        
        sql = '''
            SELECT 
                ib.*, i.sku, i.name as item_name, c.name as client_name,
                CASE 
                    WHEN ib.expiry_date < ? THEN 'Прострочено'
                    WHEN ib.expiry_date <= ? THEN 'Скоро закінчується'
                    ELSE 'В нормі'
                END as expiry_status
            FROM inventory_balances ib
            INNER JOIN items i ON ib.item_id = i.id
            INNER JOIN clients c ON ib.client_id = c.id
            WHERE ib.expiry_date IS NOT NULL
            AND ib.expiry_date <= ?
            AND (ib.qty_available > 0 OR ib.qty_reserved > 0)
        '''
        params = [today, future_date, future_date]
        
        if client_id:
            sql += ' AND ib.client_id = ?'
            params.append(client_id)
        
        sql += ' ORDER BY ib.expiry_date'
        
        rows = self.db.fetchall(sql, tuple(params))
        return [dict(row) for row in rows]
    
    def get_orders_report(self, date_from: str = None, date_to: str = None,
                         status: str = None, client_id: str = None,
                         order_type: str = 'outbound') -> List[dict]:
        """Звіт по замовленнях"""
        if order_type == 'inbound':
            table = 'inbound_orders'
            sql = '''
                SELECT io.*, c.name as client_name, s.name as supplier_name,
                       w.name as warehouse_name,
                       (SELECT COUNT(*) FROM inbound_order_lines WHERE order_id = io.id) as line_count,
                       (SELECT SUM(expected_qty) FROM inbound_order_lines WHERE order_id = io.id) as total_expected,
                       (SELECT SUM(received_qty) FROM inbound_order_lines WHERE order_id = io.id) as total_received
                FROM inbound_orders io
                INNER JOIN clients c ON io.client_id = c.id
                LEFT JOIN suppliers s ON io.supplier_id = s.id
                INNER JOIN warehouses w ON io.warehouse_id = w.id
                WHERE 1=1
            '''
        else:
            table = 'outbound_orders'
            sql = '''
                SELECT oo.*, c.name as client_name, cr.name as carrier_name,
                       w.name as warehouse_name,
                       (SELECT COUNT(*) FROM outbound_order_lines WHERE order_id = oo.id) as line_count,
                       (SELECT SUM(ordered_qty) FROM outbound_order_lines WHERE order_id = oo.id) as total_ordered,
                       (SELECT SUM(shipped_qty) FROM outbound_order_lines WHERE order_id = oo.id) as total_shipped
                FROM outbound_orders oo
                INNER JOIN clients c ON oo.client_id = c.id
                LEFT JOIN carriers cr ON oo.carrier_id = cr.id
                INNER JOIN warehouses w ON oo.warehouse_id = w.id
                WHERE 1=1
            '''
        
        params = []
        
        if date_from:
            sql += f' AND {table[0]}o.doc_date >= ?'
            params.append(date_from)
        if date_to:
            sql += f' AND {table[0]}o.doc_date <= ?'
            params.append(date_to)
        if status:
            sql += f' AND {table[0]}o.status = ?'
            params.append(status)
        if client_id:
            sql += f' AND {table[0]}o.client_id = ?'
            params.append(client_id)
        
        sql += f' ORDER BY {table[0]}o.doc_date DESC'
        
        rows = self.db.fetchall(sql, tuple(params))
        return [dict(row) for row in rows]
    
    def get_worker_productivity_report(self, date_from: str, date_to: str) -> List[dict]:
        """Звіт по продуктивності працівників"""
        sql = '''
            SELECT 
                u.username,
                u.full_name,
                sm.move_type,
                COUNT(*) as operation_count,
                SUM(ABS(sm.qty)) as total_qty
            FROM stock_moves sm
            INNER JOIN users u ON sm.user_id = u.id
            WHERE DATE(sm.created_at) BETWEEN ? AND ?
            GROUP BY u.id, sm.move_type
            ORDER BY u.username, operation_count DESC
        '''
        rows = self.db.fetchall(sql, (date_from, date_to))
        return [dict(row) for row in rows]
    
    def export_to_csv(self, data: List[dict], filename: str):
        """Експорт даних в CSV"""
        if not data:
            return
        
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys(), delimiter=';')
            writer.writeheader()
            writer.writerows(data)
        
        logger.info(f"Data exported to {filename}")


class ImportService:
    """Сервіс імпорту даних"""
    
    def __init__(self, auth_service: AuthService):
        self.auth = auth_service
        self.item_dal = ItemDAL()
        self.client_dal = ClientDAL()
        self.audit_dal = AuditDAL()
    
    def import_items_from_csv(self, filename: str) -> Tuple[int, int, List[str]]:
        """Імпорт номенклатури з CSV"""
        self.auth.check_permission('items_import')
        
        imported = 0
        errors_count = 0
        errors = []
        
        with open(filename, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=';')
            
            for row_num, row in enumerate(reader, 2):
                try:
                    sku = row.get('sku', '').strip()
                    name = row.get('name', '').strip()
                    
                    if not sku or not name:
                        errors.append(f"Рядок {row_num}: відсутній SKU або назва")
                        errors_count += 1
                        continue
                    
                    existing = self.item_dal.get_by_sku(sku)
                    if existing:
                        # Оновлюємо
                        self.item_dal.update(existing['id'], {
                            'name': name,
                            'name_en': row.get('name_en', ''),
                            'description': row.get('description', ''),
                            'weight': float(row.get('weight', 0) or 0),
                            'volume': float(row.get('volume', 0) or 0),
                        })
                    else:
                        # Створюємо
                        self.item_dal.create({
                            'sku': sku,
                            'name': name,
                            'name_en': row.get('name_en', ''),
                            'description': row.get('description', ''),
                            'weight': float(row.get('weight', 0) or 0),
                            'volume': float(row.get('volume', 0) or 0),
                        })
                    
                    # Штрихкод
                    barcode = row.get('barcode', '').strip()
                    if barcode:
                        item = self.item_dal.get_by_sku(sku)
                        existing_barcodes = self.item_dal.get_barcodes(item['id'])
                        if not any(b['barcode'] == barcode for b in existing_barcodes):
                            self.item_dal.add_barcode(item['id'], barcode)
                    
                    imported += 1
                    
                except Exception as e:
                    errors.append(f"Рядок {row_num}: {str(e)}")
                    errors_count += 1
        
        user_id = self.auth.current_user['id'] if self.auth.current_user else None
        self.audit_dal.log(
            user_id,
            self.auth.current_user['username'] if self.auth.current_user else 'system',
            'IMPORT_ITEMS',
            new_values={'imported': imported, 'errors': errors_count}
        )
        
        logger.info(f"Items import: {imported} imported, {errors_count} errors")
        return imported, errors_count, errors
    
    def import_clients_from_csv(self, filename: str) -> Tuple[int, int, List[str]]:
        """Імпорт клієнтів з CSV"""
        self.auth.check_permission('clients_edit')
        
        imported = 0
        errors_count = 0
        errors = []
        
        with open(filename, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=';')
            
            for row_num, row in enumerate(reader, 2):
                try:
                    code = row.get('code', '').strip()
                    name = row.get('name', '').strip()
                    
                    if not code or not name:
                        errors.append(f"Рядок {row_num}: відсутній код або назва")
                        errors_count += 1
                        continue
                    
                    self.client_dal.create({
                        'code': code,
                        'name': name,
                        'legal_name': row.get('legal_name', ''),
                        'tax_id': row.get('tax_id', ''),
                        'address': row.get('address', ''),
                        'phone': row.get('phone', ''),
                        'email': row.get('email', ''),
                    })
                    
                    imported += 1
                    
                except Exception as e:
                    errors.append(f"Рядок {row_num}: {str(e)}")
                    errors_count += 1
        
        logger.info(f"Clients import: {imported} imported, {errors_count} errors")
        return imported, errors_count, errors


class PrintService:
    """Сервіс друку документів"""
    
    def __init__(self):
        self.db = DatabaseManager()
    
    def _open_html(self, html: str, title: str = "Друк"):
        """Відкриває HTML у браузері для друку"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', 
                                         delete=False, encoding='utf-8') as f:
            f.write(html)
            webbrowser.open('file://' + f.name)
    
    def _html_template(self, title: str, content: str) -> str:
        """Базовий HTML шаблон"""
        return f'''
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{title}</title>
    <style>
        body {{ font-family: Arial, sans-serif; font-size: 12px; margin: 20px; }}
        h1 {{ font-size: 18px; text-align: center; }}
        h2 {{ font-size: 14px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
        th, td {{ border: 1px solid #000; padding: 5px; text-align: left; }}
        th {{ background-color: #f0f0f0; }}
        .header {{ margin-bottom: 20px; }}
        .footer {{ margin-top: 20px; font-size: 10px; }}
        .signatures {{ margin-top: 30px; }}
        .signature-line {{ display: inline-block; width: 200px; border-bottom: 1px solid #000; margin: 0 20px; }}
        @media print {{
            button {{ display: none; }}
        }}
    </style>
</head>
<body>
    <button onclick="window.print()">Друкувати</button>
    {content}
</body>
</html>
'''
    
    def print_inbound_act(self, order_id: str):
        """Друк акту приймання"""
        inbound_dal = InboundOrderDAL()
        order = inbound_dal.get_by_id(order_id)
        lines = inbound_dal.get_lines(order_id)
        
        if not order:
            raise ValueError("Замовлення не знайдено")
        
        rows_html = ''
        for idx, line in enumerate(lines, 1):
            rows_html += f'''
            <tr>
                <td>{idx}</td>
                <td>{line['sku']}</td>
                <td>{line['item_name']}</td>
                <td>{line['expected_qty']}</td>
                <td>{line['received_qty']}</td>
                <td>{line['batch_number'] or '-'}</td>
                <td>{line['expiry_date'] or '-'}</td>
            </tr>
            '''
        
        content = f'''
        <div class="header">
            <h1>АКТ ПРИЙМАННЯ ТОВАРІВ</h1>
            <p><strong>№ {order['doc_number']}</strong> від {order['doc_date']}</p>
        </div>
        
        <p><strong>Клієнт:</strong> {order['client_name']}</p>
        <p><strong>Постачальник:</strong> {order['supplier_name'] or '-'}</p>
        <p><strong>Склад:</strong> {order['warehouse_name']}</p>
        <p><strong>Статус:</strong> {order['status']}</p>
        
        <table>
            <tr>
                <th>№</th>
                <th>Артикул</th>
                <th>Найменування</th>
                <th>Очікувана к-сть</th>
                <th>Прийнята к-сть</th>
                <th>Партія</th>
                <th>Термін придатності</th>
            </tr>
            {rows_html}
        </table>
        
        <div class="signatures">
            <p>Здав: <span class="signature-line"></span></p>
            <p>Прийняв: <span class="signature-line"></span></p>
        </div>
        
        <div class="footer">
            <p>Дата друку: {now_str()}</p>
        </div>
        '''
        
        html = self._html_template(f"Акт приймання {order['doc_number']}", content)
        self._open_html(html)
    
    def print_outbound_picklist(self, order_id: str):
        """Друк листа збору"""
        outbound_dal = OutboundOrderDAL()
        order = outbound_dal.get_by_id(order_id)
        lines = outbound_dal.get_lines(order_id)
        
        if not order:
            raise ValueError("Замовлення не знайдено")
        
        rows_html = ''
        for idx, line in enumerate(lines, 1):
            rows_html += f'''
            <tr>
                <td>{idx}</td>
                <td>{line['sku']}</td>
                <td>{line['item_name']}</td>
                <td>{line['ordered_qty']}</td>
                <td></td>
                <td></td>
            </tr>
            '''
        
        content = f'''
        <div class="header">
            <h1>ЛИСТ ЗБОРУ</h1>
            <p><strong>Замовлення № {order['doc_number']}</strong> від {order['doc_date']}</p>
        </div>
        
        <p><strong>Клієнт:</strong> {order['client_name']}</p>
        <p><strong>Склад:</strong> {order['warehouse_name']}</p>
        <p><strong>Адреса доставки:</strong> {order['delivery_address'] or '-'}</p>
        <p><strong>Дата доставки:</strong> {order['delivery_date'] or '-'}</p>
        <p><strong>Перевізник:</strong> {order['carrier_name'] or '-'}</p>
        
        <table>
            <tr>
                <th>№</th>
                <th>Артикул</th>
                <th>Найменування</th>
                <th>К-сть</th>
                <th>Комірка</th>
                <th>Зібрано</th>
            </tr>
            {rows_html}
        </table>
        
        <div class="signatures">
            <p>Комірник: <span class="signature-line"></span></p>
        </div>
        
        <div class="footer">
            <p>Дата друку: {now_str()}</p>
        </div>
        '''
        
        html = self._html_template(f"Лист збору {order['doc_number']}", content)
        self._open_html(html)
    
    def print_outbound_invoice(self, order_id: str):
        """Друк видаткової накладної"""
        outbound_dal = OutboundOrderDAL()
        order = outbound_dal.get_by_id(order_id)
        lines = outbound_dal.get_lines(order_id)
        
        if not order:
            raise ValueError("Замовлення не знайдено")
        
        rows_html = ''
        total_qty = 0
        for idx, line in enumerate(lines, 1):
            rows_html += f'''
            <tr>
                <td>{idx}</td>
                <td>{line['sku']}</td>
                <td>{line['item_name']}</td>
                <td>{line['ordered_qty']}</td>
            </tr>
            '''
            total_qty += line['ordered_qty']
        
        content = f'''
        <div class="header">
            <h1>ВИДАТКОВА НАКЛАДНА</h1>
            <p><strong>№ {order['doc_number']}</strong> від {order['doc_date']}</p>
        </div>
        
        <p><strong>Відправник:</strong> Склад "{order['warehouse_name']}"</p>
        <p><strong>Отримувач:</strong> {order['client_name']}</p>
        <p><strong>Адреса доставки:</strong> {order['delivery_address'] or '-'}</p>
        <p><strong>Перевізник:</strong> {order['carrier_name'] or '-'}</p>
        
        <table>
            <tr>
                <th>№</th>
                <th>Артикул</th>
                <th>Найменування</th>
                <th>Кількість</th>
            </tr>
            {rows_html}
            <tr>
                <td colspan="3"><strong>ВСЬОГО:</strong></td>
                <td><strong>{total_qty}</strong></td>
            </tr>
        </table>
        
        <div class="signatures">
            <p>Відпустив: <span class="signature-line"></span></p>
            <p>Отримав: <span class="signature-line"></span></p>
        </div>
        
        <div class="footer">
            <p>Дата друку: {now_str()}</p>
        </div>
        '''
        
        html = self._html_template(f"Накладна {order['doc_number']}", content)
        self._open_html(html)
    
    def print_writeoff_act(self, writeoff_id: str):
        """Друк акту списання"""
        writeoff_dal = WriteoffDAL()
        writeoff = writeoff_dal.get_by_id(writeoff_id)
        lines = writeoff_dal.get_lines(writeoff_id)
        
        if not writeoff:
            raise ValueError("Списання не знайдено")
        
        rows_html = ''
        for idx, line in enumerate(lines, 1):
            rows_html += f'''
            <tr>
                <td>{idx}</td>
                <td>{line['sku']}</td>
                <td>{line['item_name']}</td>
                <td>{line['qty']}</td>
                <td>{line['reason'] or '-'}</td>
            </tr>
            '''
        
        content = f'''
        <div class="header">
            <h1>АКТ СПИСАННЯ</h1>
            <p><strong>№ {writeoff['doc_number']}</strong> від {writeoff['doc_date']}</p>
        </div>
        
        <p><strong>Клієнт:</strong> {writeoff['client_name']}</p>
        <p><strong>Склад:</strong> {writeoff['warehouse_name']}</p>
        <p><strong>Причина:</strong> {writeoff['reason'] or '-'}</p>
        
        <table>
            <tr>
                <th>№</th>
                <th>Артикул</th>
                <th>Найменування</th>
                <th>Кількість</th>
                <th>Причина</th>
            </tr>
            {rows_html}
        </table>
        
        <div class="signatures">
            <p>Комірник: <span class="signature-line"></span></p>
            <p>Затвердив: <span class="signature-line"></span></p>
        </div>
        
        <div class="footer">
            <p>Дата друку: {now_str()}</p>
        </div>
        '''
        
        html = self._html_template(f"Акт списання {writeoff['doc_number']}", content)
        self._open_html(html)
    
    def print_item_label(self, item_id: str):
        """Друк етикетки товару"""
        item_dal = ItemDAL()
        item = item_dal.get_by_id(item_id)
        barcodes = item_dal.get_barcodes(item_id)
        
        if not item:
            raise ValueError("Товар не знайдено")
        
        barcode = barcodes[0]['barcode'] if barcodes else 'N/A'
        
        content = f'''
        <div style="width: 300px; padding: 10px; border: 2px solid #000;">
            <h2 style="margin: 0; text-align: center;">{item['name']}</h2>
            <p style="text-align: center; font-size: 16px; margin: 5px 0;"><strong>{item['sku']}</strong></p>
            <p style="text-align: center; font-size: 24px; font-family: 'Libre Barcode 39', monospace; margin: 10px 0;">
                *{barcode}*
            </p>
            <p style="text-align: center; margin: 0;">{barcode}</p>
        </div>
        '''
        
        html = self._html_template(f"Етикетка {item['sku']}", content)
        self._open_html(html)


# ============================================================================
# UI КОМПОНЕНТИ
# ============================================================================

class BaseFrame(ttk.Frame):
    """Базовий фрейм"""
    
    def __init__(self, parent, app):
        super().__init__(parent)
        self.app = app
        self.auth = app.auth_service
    
    def show_error(self, message: str):
        messagebox.showerror("Помилка", message)
    
    def show_info(self, message: str):
        messagebox.showinfo("Інформація", message)
    
    def show_warning(self, message: str):
        messagebox.showwarning("Увага", message)
    
    def ask_confirm(self, message: str) -> bool:
        return messagebox.askyesno("Підтвердження", message)


class DataTable(ttk.Frame):
    """Компонент таблиці даних"""
    
    def __init__(self, parent, columns: List[Tuple[str, str, int]], 
                 on_double_click=None, on_select=None):
        super().__init__(parent)
        
        self.columns = columns
        self.on_double_click = on_double_click
        self.on_select = on_select
        self.data = []
        
        # Створюємо таблицю
        col_ids = [c[0] for c in columns]
        self.tree = ttk.Treeview(self, columns=col_ids, show='headings', 
                                  selectmode='browse')
        
        # Налаштовуємо колонки
        for col_id, col_name, col_width in columns:
            self.tree.heading(col_id, text=col_name, 
                            command=lambda c=col_id: self._sort_column(c))
            self.tree.column(col_id, width=col_width, minwidth=50)
        
        # Скролбари
        vsb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        
        # Розміщення
        self.tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')
        
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        
        # Події
        if on_double_click:
            self.tree.bind('<Double-1>', self._on_double_click)
        if on_select:
            self.tree.bind('<<TreeviewSelect>>', self._on_select)
        
        # Альтернативні кольори рядків
        self.tree.tag_configure('oddrow', background=COLORS['row_alt'])
    
    def load_data(self, data: List[dict]):
        """Завантаження даних"""
        self.data = data
        self.tree.delete(*self.tree.get_children())
        
        col_ids = [c[0] for c in self.columns]
        for idx, row in enumerate(data):
            values = [row.get(col_id, '') for col_id in col_ids]
            tag = 'oddrow' if idx % 2 else ''
            self.tree.insert('', 'end', values=values, tags=(tag,))
    
    def get_selected(self) -> Optional[dict]:
        """Отримання вибраного рядка"""
        selection = self.tree.selection()
        if not selection:
            return None
        
        item = self.tree.item(selection[0])
        values = item['values']
        col_ids = [c[0] for c in self.columns]
        
        return dict(zip(col_ids, values))
    
    def get_selected_index(self) -> int:
        """Отримання індексу вибраного рядка"""
        selection = self.tree.selection()
        if not selection:
            return -1
        return self.tree.index(selection[0])
    
    def _on_double_click(self, event):
        if self.on_double_click:
            selected = self.get_selected()
            if selected:
                self.on_double_click(selected)
    
    def _on_select(self, event):
        if self.on_select:
            selected = self.get_selected()
            self.on_select(selected)
    
    def _sort_column(self, col):
        """Сортування по колонці"""
        data = [(self.tree.set(item, col), item) for item in self.tree.get_children('')]
        data.sort(reverse=getattr(self, '_sort_reverse', False))
        
        for idx, (val, item) in enumerate(data):
            self.tree.move(item, '', idx)
        
        self._sort_reverse = not getattr(self, '_sort_reverse', False)


class ToolbarButton(ttk.Button):
    """Кнопка панелі інструментів"""
    
    def __init__(self, parent, text, command, **kwargs):
        super().__init__(parent, text=text, command=command, **kwargs)


class FilterPanel(ttk.Frame):
    """Панель фільтрів"""
    
    def __init__(self, parent, filters: List[dict], on_apply):
        super().__init__(parent)
        self.filters = {}
        self.on_apply = on_apply
        
        for idx, f in enumerate(filters):
            ttk.Label(self, text=f['label']).grid(row=0, column=idx*2, padx=5, pady=5)
            
            if f['type'] == 'combo':
                var = tk.StringVar()
                widget = ttk.Combobox(self, textvariable=var, values=f.get('values', []),
                                     state='readonly', width=15)
                if f.get('values'):
                    widget.set(f['values'][0] if f['values'] else '')
            elif f['type'] == 'date':
                var = tk.StringVar(value=f.get('default', ''))
                widget = ttk.Entry(self, textvariable=var, width=12)
            else:
                var = tk.StringVar(value=f.get('default', ''))
                widget = ttk.Entry(self, textvariable=var, width=15)
            
            widget.grid(row=0, column=idx*2+1, padx=5, pady=5)
            self.filters[f['name']] = var
        
        ttk.Button(self, text="Застосувати", command=self._apply).grid(
            row=0, column=len(filters)*2, padx=10, pady=5)
        ttk.Button(self, text="Скинути", command=self._reset).grid(
            row=0, column=len(filters)*2+1, padx=5, pady=5)
    
    def _apply(self):
        values = {name: var.get() for name, var in self.filters.items()}
        self.on_apply(values)
    
    def _reset(self):
        for var in self.filters.values():
            var.set('')
        self.on_apply({})
    
    def get_values(self) -> dict:
        return {name: var.get() for name, var in self.filters.items()}


# ============================================================================
# ФОРМИ ДОВІДНИКІВ
# ============================================================================

class ItemListFrame(BaseFrame):
    """Список номенклатури"""
    
    def __init__(self, parent, app):
        super().__init__(parent, app)
        self.item_dal = ItemDAL()
        self._create_widgets()
        self._load_data()
    
    def _create_widgets(self):
        # Панель інструментів
        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', padx=5, pady=5)
        
        ttk.Button(toolbar, text="➕ Створити", command=self._create).pack(side='left', padx=2)
        ttk.Button(toolbar, text="✏️ Редагувати", command=self._edit).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🗑️ Видалити", command=self._delete).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🔄 Оновити", command=self._load_data).pack(side='left', padx=2)
        ttk.Button(toolbar, text="📥 Імпорт CSV", command=self._import).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🏷️ Друк етикетки", command=self._print_label).pack(side='left', padx=2)
        
        # Пошук
        search_frame = ttk.Frame(self)
        search_frame.pack(fill='x', padx=5, pady=2)
        
        ttk.Label(search_frame, text="Пошук:").pack(side='left')
        self.search_var = tk.StringVar()
        self.search_var.trace('w', lambda *args: self._load_data())
        ttk.Entry(search_frame, textvariable=self.search_var, width=30).pack(side='left', padx=5)
        
        # Таблиця
        columns = [
            ('id', 'ID', 0),
            ('sku', 'Артикул', 100),
            ('name', 'Найменування', 250),
            ('category_name', 'Категорія', 120),
            ('uom_name', 'Од. виміру', 80),
            ('weight', 'Вага', 70),
            ('is_active', 'Активний', 70),
        ]
        self.table = DataTable(self, columns, on_double_click=self._edit_selected)
        self.table.pack(fill='both', expand=True, padx=5, pady=5)
    
    def _load_data(self):
        search = self.search_var.get() if hasattr(self, 'search_var') else None
        data = self.item_dal.get_all(active_only=False, search=search)
        # Конвертуємо is_active
        for row in data:
            row['is_active'] = 'Так' if row['is_active'] else 'Ні'
        self.table.load_data(data)
    
    def _create(self):
        if not self.auth.has_permission('items_edit'):
            self.show_error("Недостатньо прав")
            return
        ItemEditDialog(self, self.app, None, self._load_data)
    
    def _edit(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть товар")
            return
        self._edit_selected(selected)
    
    def _edit_selected(self, selected):
        if not self.auth.has_permission('items_edit'):
            self.show_error("Недостатньо прав")
            return
        
        idx = self.table.get_selected_index()
        if idx >= 0:
            item_id = self.table.data[idx]['id']
            ItemEditDialog(self, self.app, item_id, self._load_data)
    
    def _delete(self):
        if not self.auth.has_permission('items_delete'):
            self.show_error("Недостатньо прав")
            return
        
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть товар")
            return
        
        if not self.ask_confirm(f"Видалити товар {selected['sku']}?"):
            return
        
        try:
            idx = self.table.get_selected_index()
            item_id = self.table.data[idx]['id']
            self.item_dal.delete(item_id)
            self.show_info("Товар видалено")
            self._load_data()
        except Exception as e:
            self.show_error(str(e))
    
    def _import(self):
        if not self.auth.has_permission('items_import'):
            self.show_error("Недостатньо прав")
            return
        
        filename = filedialog.askopenfilename(
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if not filename:
            return
        
        try:
            import_service = ImportService(self.auth)
            imported, errors_count, errors = import_service.import_items_from_csv(filename)
            
            msg = f"Імпортовано: {imported}\nПомилок: {errors_count}"
            if errors:
                msg += "\n\nПомилки:\n" + "\n".join(errors[:10])
            
            self.show_info(msg)
            self._load_data()
        except Exception as e:
            self.show_error(str(e))
    
    def _print_label(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть товар")
            return
        
        try:
            idx = self.table.get_selected_index()
            item_id = self.table.data[idx]['id']
            PrintService().print_item_label(item_id)
        except Exception as e:
            self.show_error(str(e))


class ItemEditDialog(tk.Toplevel):
    """Діалог редагування товару"""
    
    def __init__(self, parent, app, item_id: str = None, on_save=None):
        super().__init__(parent)
        self.app = app
        self.item_id = item_id
        self.on_save = on_save
        self.item_dal = ItemDAL()
        self.category_dal = CategoryDAL()
        self.uom_dal = UomDAL()
        
        self.title("Картка номенклатури" if item_id else "Новий товар")
        self.geometry("700x550")
        self.transient(parent)
        self.grab_set()
        
        self._create_widgets()
        if item_id:
            self._load_data()
    
    def _create_widgets(self):
        # Notebook з вкладками
        notebook = ttk.Notebook(self)
        notebook.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Вкладка "Основне"
        main_frame = ttk.Frame(notebook)
        notebook.add(main_frame, text="Основне")
        
        # Артикул
        ttk.Label(main_frame, text="Артикул (SKU):").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.sku_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.sku_var, width=30).grid(row=0, column=1, sticky='w', padx=5, pady=5)
        
        # Найменування
        ttk.Label(main_frame, text="Найменування:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.name_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.name_var, width=50).grid(row=1, column=1, columnspan=2, sticky='w', padx=5, pady=5)
        
        # Найменування (англ)
        ttk.Label(main_frame, text="Найменування (англ):").grid(row=2, column=0, sticky='e', padx=5, pady=5)
        self.name_en_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.name_en_var, width=50).grid(row=2, column=1, columnspan=2, sticky='w', padx=5, pady=5)
        
        # Категорія
        ttk.Label(main_frame, text="Категорія:").grid(row=3, column=0, sticky='e', padx=5, pady=5)
        self.category_var = tk.StringVar()
        categories = self.category_dal.get_all()
        self.categories_map = {c['name']: c['id'] for c in categories}
        self.category_combo = ttk.Combobox(main_frame, textvariable=self.category_var, 
                                           values=list(self.categories_map.keys()), state='readonly', width=27)
        self.category_combo.grid(row=3, column=1, sticky='w', padx=5, pady=5)
        
        # Одиниця виміру
        ttk.Label(main_frame, text="Одиниця виміру:").grid(row=4, column=0, sticky='e', padx=5, pady=5)
        self.uom_var = tk.StringVar()
        uoms = self.uom_dal.get_all()
        self.uoms_map = {u['name']: u['id'] for u in uoms}
        self.uom_combo = ttk.Combobox(main_frame, textvariable=self.uom_var,
                                      values=list(self.uoms_map.keys()), state='readonly', width=27)
        self.uom_combo.grid(row=4, column=1, sticky='w', padx=5, pady=5)
        
        # Опис
        ttk.Label(main_frame, text="Опис:").grid(row=5, column=0, sticky='ne', padx=5, pady=5)
        self.description_text = tk.Text(main_frame, width=50, height=4)
        self.description_text.grid(row=5, column=1, columnspan=2, sticky='w', padx=5, pady=5)
        
        # Активний
        self.is_active_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(main_frame, text="Активний", variable=self.is_active_var).grid(
            row=6, column=1, sticky='w', padx=5, pady=5)
        
        # Вкладка "Характеристики"
        char_frame = ttk.Frame(notebook)
        notebook.add(char_frame, text="Характеристики")
        
        # Вага
        ttk.Label(char_frame, text="Вага (кг):").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.weight_var = tk.StringVar(value="0")
        ttk.Entry(char_frame, textvariable=self.weight_var, width=15).grid(row=0, column=1, sticky='w', padx=5, pady=5)
        
        # Об'єм
        ttk.Label(char_frame, text="Об'єм (м³):").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.volume_var = tk.StringVar(value="0")
        ttk.Entry(char_frame, textvariable=self.volume_var, width=15).grid(row=1, column=1, sticky='w', padx=5, pady=5)
        
        # Габарити
        ttk.Label(char_frame, text="Довжина (см):").grid(row=2, column=0, sticky='e', padx=5, pady=5)
        self.length_var = tk.StringVar(value="0")
        ttk.Entry(char_frame, textvariable=self.length_var, width=15).grid(row=2, column=1, sticky='w', padx=5, pady=5)
        
        ttk.Label(char_frame, text="Ширина (см):").grid(row=3, column=0, sticky='e', padx=5, pady=5)
        self.width_var = tk.StringVar(value="0")
        ttk.Entry(char_frame, textvariable=self.width_var, width=15).grid(row=3, column=1, sticky='w', padx=5, pady=5)
        
        ttk.Label(char_frame, text="Висота (см):").grid(row=4, column=0, sticky='e', padx=5, pady=5)
        self.height_var = tk.StringVar(value="0")
        ttk.Entry(char_frame, textvariable=self.height_var, width=15).grid(row=4, column=1, sticky='w', padx=5, pady=5)
        
        # Вкладка "Зберігання"
        storage_frame = ttk.Frame(notebook)
        notebook.add(storage_frame, text="Зберігання")
        
        # Серійний облік
        self.is_serialized_var = tk.BooleanVar()
        ttk.Checkbutton(storage_frame, text="Серійний облік", variable=self.is_serialized_var).grid(
            row=0, column=0, sticky='w', padx=5, pady=5)
        
        # Партійний облік
        self.is_batch_var = tk.BooleanVar()
        ttk.Checkbutton(storage_frame, text="Партійний облік", variable=self.is_batch_var).grid(
            row=1, column=0, sticky='w', padx=5, pady=5)
        
        # Облік терміну придатності
        self.is_expiry_var = tk.BooleanVar()
        ttk.Checkbutton(storage_frame, text="Облік терміну придатності", variable=self.is_expiry_var).grid(
            row=2, column=0, sticky='w', padx=5, pady=5)
        
        # Мін. термін придатності
        ttk.Label(storage_frame, text="Мін. термін придатності (днів):").grid(row=3, column=0, sticky='e', padx=5, pady=5)
        self.min_expiry_var = tk.StringVar(value="0")
        ttk.Entry(storage_frame, textvariable=self.min_expiry_var, width=10).grid(row=3, column=1, sticky='w', padx=5, pady=5)
        
        # Температурний режим
        ttk.Label(storage_frame, text="Температура (від):").grid(row=4, column=0, sticky='e', padx=5, pady=5)
        self.temp_min_var = tk.StringVar()
        ttk.Entry(storage_frame, textvariable=self.temp_min_var, width=10).grid(row=4, column=1, sticky='w', padx=5, pady=5)
        
        ttk.Label(storage_frame, text="Температура (до):").grid(row=5, column=0, sticky='e', padx=5, pady=5)
        self.temp_max_var = tk.StringVar()
        ttk.Entry(storage_frame, textvariable=self.temp_max_var, width=10).grid(row=5, column=1, sticky='w', padx=5, pady=5)
        
        # Вкладка "Штрихкоди"
        barcode_frame = ttk.Frame(notebook)
        notebook.add(barcode_frame, text="Штрихкоди")
        
        # Таблиця штрихкодів
        bc_columns = [('id', 'ID', 0), ('barcode', 'Штрихкод', 200), ('barcode_type', 'Тип', 100)]
        self.barcode_table = DataTable(barcode_frame, bc_columns)
        self.barcode_table.pack(fill='both', expand=True, padx=5, pady=5)
        
        bc_btn_frame = ttk.Frame(barcode_frame)
        bc_btn_frame.pack(fill='x', padx=5, pady=5)
        ttk.Button(bc_btn_frame, text="Додати", command=self._add_barcode).pack(side='left', padx=2)
        ttk.Button(bc_btn_frame, text="Видалити", command=self._remove_barcode).pack(side='left', padx=2)
        
        # Кнопки
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(btn_frame, text="Записати", command=self._save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Записати і закрити", command=self._save_and_close).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Закрити", command=self.destroy).pack(side='right', padx=5)
    
    def _load_data(self):
        item = self.item_dal.get_by_id(self.item_id)
        if not item:
            return
        
        self.sku_var.set(item['sku'])
        self.name_var.set(item['name'])
        self.name_en_var.set(item['name_en'] or '')
        
        # Категорія
        if item['category_name']:
            self.category_var.set(item['category_name'])
        
        # Одиниця виміру
        if item['uom_name']:
            self.uom_var.set(item['uom_name'])
            self.description_text.delete('1.0', tk.END)
            self.description_text.insert('1.0', item['description'] or '')
        
        self.is_active_var.set(bool(item['is_active']))
        self.weight_var.set(str(item['weight'] or 0))
        self.volume_var.set(str(item['volume'] or 0))
        self.length_var.set(str(item['length'] or 0))
        self.width_var.set(str(item['width'] or 0))
        self.height_var.set(str(item['height'] or 0))
        
        self.is_serialized_var.set(bool(item['is_serialized']))
        self.is_batch_var.set(bool(item['is_batch_tracked']))
        self.is_expiry_var.set(bool(item['is_expiry_tracked']))
        self.min_expiry_var.set(str(item['min_expiry_days'] or 0))
        self.temp_min_var.set(str(item['storage_temp_min']) if item['storage_temp_min'] else '')
        self.temp_max_var.set(str(item['storage_temp_max']) if item['storage_temp_max'] else '')
        
        # Завантажуємо штрихкоди
        barcodes = self.item_dal.get_barcodes(self.item_id)
        self.barcode_table.load_data(barcodes)
    
    def _get_data(self) -> dict:
        return {
            'sku': self.sku_var.get().strip(),
            'name': self.name_var.get().strip(),
            'name_en': self.name_en_var.get().strip() or None,
            'description': self.description_text.get('1.0', tk.END).strip() or None,
            'category_id': self.categories_map.get(self.category_var.get()),
            'uom_id': self.uoms_map.get(self.uom_var.get()),
            'weight': float(self.weight_var.get() or 0),
            'volume': float(self.volume_var.get() or 0),
            'length': float(self.length_var.get() or 0),
            'width': float(self.width_var.get() or 0),
            'height': float(self.height_var.get() or 0),
            'is_serialized': 1 if self.is_serialized_var.get() else 0,
            'is_batch_tracked': 1 if self.is_batch_var.get() else 0,
            'is_expiry_tracked': 1 if self.is_expiry_var.get() else 0,
            'min_expiry_days': int(self.min_expiry_var.get() or 0),
            'storage_temp_min': float(self.temp_min_var.get()) if self.temp_min_var.get() else None,
            'storage_temp_max': float(self.temp_max_var.get()) if self.temp_max_var.get() else None,
            'is_active': 1 if self.is_active_var.get() else 0,
        }
    
    def _validate(self) -> bool:
        data = self._get_data()
        if not data['sku']:
            messagebox.showerror("Помилка", "Введіть артикул")
            return False
        if not data['name']:
            messagebox.showerror("Помилка", "Введіть найменування")
            return False
        return True
    
    def _save(self) -> bool:
        if not self._validate():
            return False
        
        try:
            data = self._get_data()
            if self.item_id:
                self.item_dal.update(self.item_id, data)
            else:
                self.item_id = self.item_dal.create(data)
            
            messagebox.showinfo("Інформація", "Збережено")
            if self.on_save:
                self.on_save()
            return True
        except Exception as e:
            messagebox.showerror("Помилка", str(e))
            return False
    
    def _save_and_close(self):
        if self._save():
            self.destroy()
    
    def _add_barcode(self):
        if not self.item_id:
            messagebox.showwarning("Увага", "Спочатку збережіть товар")
            return
        
        barcode = simpledialog.askstring("Штрихкод", "Введіть штрихкод:")
        if barcode:
            try:
                self.item_dal.add_barcode(self.item_id, barcode.strip())
                barcodes = self.item_dal.get_barcodes(self.item_id)
                self.barcode_table.load_data(barcodes)
            except Exception as e:
                messagebox.showerror("Помилка", str(e))
    
    def _remove_barcode(self):
        selected = self.barcode_table.get_selected()
        if not selected:
            messagebox.showwarning("Увага", "Виберіть штрихкод")
            return
        
        idx = self.barcode_table.get_selected_index()
        barcode_id = self.barcode_table.data[idx]['id']
        
        try:
            self.item_dal.remove_barcode(barcode_id)
            barcodes = self.item_dal.get_barcodes(self.item_id)
            self.barcode_table.load_data(barcodes)
        except Exception as e:
            messagebox.showerror("Помилка", str(e))


class ClientListFrame(BaseFrame):
    """Список клієнтів 3PL"""
    
    def __init__(self, parent, app):
        super().__init__(parent, app)
        self.client_dal = ClientDAL()
        self._create_widgets()
        self._load_data()
    
    def _create_widgets(self):
        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', padx=5, pady=5)
        
        ttk.Button(toolbar, text="➕ Створити", command=self._create).pack(side='left', padx=2)
        ttk.Button(toolbar, text="✏️ Редагувати", command=self._edit).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🗑️ Видалити", command=self._delete).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🔄 Оновити", command=self._load_data).pack(side='left', padx=2)
        
        columns = [
            ('id', 'ID', 0),
            ('code', 'Код', 80),
            ('name', 'Найменування', 200),
            ('legal_name', 'Юр. назва', 200),
            ('tax_id', 'ЄДРПОУ', 100),
            ('phone', 'Телефон', 120),
            ('email', 'Email', 150),
            ('is_active', 'Активний', 70),
        ]
        self.table = DataTable(self, columns, on_double_click=self._edit_selected)
        self.table.pack(fill='both', expand=True, padx=5, pady=5)
    
    def _load_data(self):
        data = self.client_dal.get_all(active_only=False)
        for row in data:
            row['is_active'] = 'Так' if row['is_active'] else 'Ні'
        self.table.load_data(data)
    
    def _create(self):
        if not self.auth.has_permission('clients_edit'):
            self.show_error("Недостатньо прав")
            return
        ClientEditDialog(self, self.app, None, self._load_data)
    
    def _edit(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть клієнта")
            return
        self._edit_selected(selected)
    
    def _edit_selected(self, selected):
        if not self.auth.has_permission('clients_edit'):
            self.show_error("Недостатньо прав")
            return
        idx = self.table.get_selected_index()
        if idx >= 0:
            client_id = self.table.data[idx]['id']
            ClientEditDialog(self, self.app, client_id, self._load_data)
    
    def _delete(self):
        if not self.auth.has_permission('clients_delete'):
            self.show_error("Недостатньо прав")
            return
        
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть клієнта")
            return
        
        if not self.ask_confirm(f"Видалити клієнта {selected['name']}?"):
            return
        
        try:
            idx = self.table.get_selected_index()
            client_id = self.table.data[idx]['id']
            self.client_dal.delete(client_id)
            self.show_info("Клієнта видалено")
            self._load_data()
        except Exception as e:
            self.show_error(str(e))


class ClientEditDialog(tk.Toplevel):
    """Діалог редагування клієнта"""
    
    def __init__(self, parent, app, client_id: str = None, on_save=None):
        super().__init__(parent)
        self.app = app
        self.client_id = client_id
        self.on_save = on_save
        self.client_dal = ClientDAL()
        
        self.title("Картка клієнта" if client_id else "Новий клієнт")
        self.geometry("500x450")
        self.transient(parent)
        self.grab_set()
        
        self._create_widgets()
        if client_id:
            self._load_data()
    
    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(fill='both', expand=True)
        
        # Код
        ttk.Label(main_frame, text="Код:").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.code_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.code_var, width=20).grid(row=0, column=1, sticky='w', padx=5, pady=5)
        
        # Найменування
        ttk.Label(main_frame, text="Найменування:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.name_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.name_var, width=40).grid(row=1, column=1, sticky='w', padx=5, pady=5)
        
        # Юр. назва
        ttk.Label(main_frame, text="Юр. назва:").grid(row=2, column=0, sticky='e', padx=5, pady=5)
        self.legal_name_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.legal_name_var, width=40).grid(row=2, column=1, sticky='w', padx=5, pady=5)
        
        # ЄДРПОУ
        ttk.Label(main_frame, text="ЄДРПОУ:").grid(row=3, column=0, sticky='e', padx=5, pady=5)
        self.tax_id_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.tax_id_var, width=20).grid(row=3, column=1, sticky='w', padx=5, pady=5)
        
        # Адреса
        ttk.Label(main_frame, text="Адреса:").grid(row=4, column=0, sticky='e', padx=5, pady=5)
        self.address_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.address_var, width=40).grid(row=4, column=1, sticky='w', padx=5, pady=5)
        
        # Телефон
        ttk.Label(main_frame, text="Телефон:").grid(row=5, column=0, sticky='e', padx=5, pady=5)
        self.phone_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.phone_var, width=20).grid(row=5, column=1, sticky='w', padx=5, pady=5)
        
        # Email
        ttk.Label(main_frame, text="Email:").grid(row=6, column=0, sticky='e', padx=5, pady=5)
        self.email_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.email_var, width=30).grid(row=6, column=1, sticky='w', padx=5, pady=5)
        
        # Номер договору
        ttk.Label(main_frame, text="№ договору:").grid(row=7, column=0, sticky='e', padx=5, pady=5)
        self.contract_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.contract_var, width=20).grid(row=7, column=1, sticky='w', padx=5, pady=5)
        
        # SLA
        ttk.Label(main_frame, text="SLA (днів):").grid(row=8, column=0, sticky='e', padx=5, pady=5)
        self.sla_var = tk.StringVar(value="3")
        ttk.Entry(main_frame, textvariable=self.sla_var, width=10).grid(row=8, column=1, sticky='w', padx=5, pady=5)
        
        # Тип тарифікації
        ttk.Label(main_frame, text="Тарифікація:").grid(row=9, column=0, sticky='e', padx=5, pady=5)
        self.tariff_var = tk.StringVar(value="operations")
        ttk.Combobox(main_frame, textvariable=self.tariff_var, 
                    values=['operations', 'pallets', 'locations'], state='readonly', width=17).grid(row=9, column=1, sticky='w', padx=5, pady=5)
        
        # Активний
        self.is_active_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(main_frame, text="Активний", variable=self.is_active_var).grid(row=10, column=1, sticky='w', padx=5, pady=5)
        
        # Кнопки
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(btn_frame, text="Зберегти", command=self._save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Закрити", command=self.destroy).pack(side='right', padx=5)
    
    def _load_data(self):
        client = self.client_dal.get_by_id(self.client_id)
        if not client:
            return
        
        self.code_var.set(client['code'])
        self.name_var.set(client['name'])
        self.legal_name_var.set(client['legal_name'] or '')
        self.tax_id_var.set(client['tax_id'] or '')
        self.address_var.set(client['address'] or '')
        self.phone_var.set(client['phone'] or '')
        self.email_var.set(client['email'] or '')
        self.contract_var.set(client['contract_number'] or '')
        self.sla_var.set(str(client['sla_days'] or 3))
        self.tariff_var.set(client['tariff_type'] or 'operations')
        self.is_active_var.set(bool(client['is_active']))
    
    def _save(self):
        code = self.code_var.get().strip()
        name = self.name_var.get().strip()
        
        if not code or not name:
            messagebox.showerror("Помилка", "Заповніть обов'язкові поля")
            return
        
        try:
            data = {
                'code': code,
                'name': name,
                'legal_name': self.legal_name_var.get().strip() or None,
                'tax_id': self.tax_id_var.get().strip() or None,
                'address': self.address_var.get().strip() or None,
                'phone': self.phone_var.get().strip() or None,
                'email': self.email_var.get().strip() or None,
                'contract_number': self.contract_var.get().strip() or None,
                'sla_days': int(self.sla_var.get() or 3),
                'tariff_type': self.tariff_var.get(),
                'is_active': 1 if self.is_active_var.get() else 0,
            }
            
            if self.client_id:
                self.client_dal.update(self.client_id, data)
            else:
                self.client_dal.create(data)
            
            messagebox.showinfo("Інформація", "Збережено")
            if self.on_save:
                self.on_save()
            self.destroy()
        except Exception as e:
            messagebox.showerror("Помилка", str(e))


class SupplierListFrame(BaseFrame):
    """Список постачальників"""
    
    def __init__(self, parent, app):
        super().__init__(parent, app)
        self.supplier_dal = SupplierDAL()
        self._create_widgets()
        self._load_data()
    
    def _create_widgets(self):
        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', padx=5, pady=5)
        
        ttk.Button(toolbar, text="➕ Створити", command=self._create).pack(side='left', padx=2)
        ttk.Button(toolbar, text="✏️ Редагувати", command=self._edit).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🗑️ Видалити", command=self._delete).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🔄 Оновити", command=self._load_data).pack(side='left', padx=2)
        
        columns = [
            ('id', 'ID', 0),
            ('code', 'Код', 80),
            ('name', 'Найменування', 200),
            ('legal_name', 'Юр. назва', 200),
            ('phone', 'Телефон', 120),
            ('email', 'Email', 150),
        ]
        self.table = DataTable(self, columns, on_double_click=self._edit_selected)
        self.table.pack(fill='both', expand=True, padx=5, pady=5)
    
    def _load_data(self):
        data = self.supplier_dal.get_all(active_only=False)
        self.table.load_data(data)
    
    def _create(self):
        if not self.auth.has_permission('suppliers_edit'):
            self.show_error("Недостатньо прав")
            return
        SupplierEditDialog(self, self.app, None, self._load_data)
    
    def _edit(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть постачальника")
            return
        self._edit_selected(selected)
    
    def _edit_selected(self, selected):
        if not self.auth.has_permission('suppliers_edit'):
            self.show_error("Недостатньо прав")
            return
        idx = self.table.get_selected_index()
        if idx >= 0:
            supplier_id = self.table.data[idx]['id']
            SupplierEditDialog(self, self.app, supplier_id, self._load_data)
    
    def _delete(self):
        if not self.auth.has_permission('suppliers_delete'):
            self.show_error("Недостатньо прав")
            return
        
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть постачальника")
            return
        
        if not self.ask_confirm(f"Видалити постачальника {selected['name']}?"):
            return
        
        try:
            idx = self.table.get_selected_index()
            supplier_id = self.table.data[idx]['id']
            self.supplier_dal.delete(supplier_id)
            self.show_info("Постачальника видалено")
            self._load_data()
        except Exception as e:
            self.show_error(str(e))


class SupplierEditDialog(tk.Toplevel):
    """Діалог редагування постачальника"""
    
    def __init__(self, parent, app, supplier_id: str = None, on_save=None):
        super().__init__(parent)
        self.app = app
        self.supplier_id = supplier_id
        self.on_save = on_save
        self.supplier_dal = SupplierDAL()
        
        self.title("Картка постачальника" if supplier_id else "Новий постачальник")
        self.geometry("450x350")
        self.transient(parent)
        self.grab_set()
        
        self._create_widgets()
        if supplier_id:
            self._load_data()
    
    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(fill='both', expand=True)
        
        fields = [
            ("Код:", "code", 20),
            ("Найменування:", "name", 40),
            ("Юр. назва:", "legal_name", 40),
            ("ЄДРПОУ:", "tax_id", 20),
            ("Адреса:", "address", 40),
            ("Телефон:", "phone", 20),
            ("Email:", "email", 30),
        ]
        
        self.vars = {}
        for idx, (label, name, width) in enumerate(fields):
            ttk.Label(main_frame, text=label).grid(row=idx, column=0, sticky='e', padx=5, pady=5)
            var = tk.StringVar()
            ttk.Entry(main_frame, textvariable=var, width=width).grid(row=idx, column=1, sticky='w', padx=5, pady=5)
            self.vars[name] = var
        
        self.is_active_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(main_frame, text="Активний", variable=self.is_active_var).grid(
            row=len(fields), column=1, sticky='w', padx=5, pady=5)
        
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(btn_frame, text="Зберегти", command=self._save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Закрити", command=self.destroy).pack(side='right', padx=5)
    
    def _load_data(self):
        supplier = self.supplier_dal.get_by_id(self.supplier_id)
        if not supplier:
            return
        
        for name, var in self.vars.items():
            var.set(supplier.get(name) or '')
        self.is_active_var.set(bool(supplier['is_active']))
    
    def _save(self):
        code = self.vars['code'].get().strip()
        name = self.vars['name'].get().strip()
        
        if not code or not name:
            messagebox.showerror("Помилка", "Заповніть обов'язкові поля")
            return
        
        try:
            data = {k: v.get().strip() or None for k, v in self.vars.items()}
            data['is_active'] = 1 if self.is_active_var.get() else 0
            
            if self.supplier_id:
                self.supplier_dal.update(self.supplier_id, data)
            else:
                self.supplier_dal.create(data)
            
            messagebox.showinfo("Інформація", "Збережено")
            if self.on_save:
                self.on_save()
            self.destroy()
        except Exception as e:
            messagebox.showerror("Помилка", str(e))


class WarehouseListFrame(BaseFrame):
    """Список складів"""
    
    def __init__(self, parent, app):
        super().__init__(parent, app)
        self.warehouse_dal = WarehouseDAL()
        self._create_widgets()
        self._load_data()
    
    def _create_widgets(self):
        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', padx=5, pady=5)
        
        ttk.Button(toolbar, text="➕ Створити", command=self._create).pack(side='left', padx=2)
        ttk.Button(toolbar, text="✏️ Редагувати", command=self._edit).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🔄 Оновити", command=self._load_data).pack(side='left', padx=2)
        
        columns = [
            ('id', 'ID', 0),
            ('code', 'Код', 100),
            ('name', 'Найменування', 250),
            ('address', 'Адреса', 300),
            ('is_active', 'Активний', 80),
        ]
        self.table = DataTable(self, columns, on_double_click=self._edit_selected)
        self.table.pack(fill='both', expand=True, padx=5, pady=5)
    
    def _load_data(self):
        data = self.warehouse_dal.get_all(active_only=False)
        for row in data:
            row['is_active'] = 'Так' if row['is_active'] else 'Ні'
        self.table.load_data(data)
    
    def _create(self):
        if not self.auth.has_permission('warehouses_edit'):
            self.show_error("Недостатньо прав")
            return
        WarehouseEditDialog(self, self.app, None, self._load_data)
    
    def _edit(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть склад")
            return
        self._edit_selected(selected)
    
    def _edit_selected(self, selected):
        if not self.auth.has_permission('warehouses_edit'):
            self.show_error("Недостатньо прав")
            return
        idx = self.table.get_selected_index()
        if idx >= 0:
            warehouse_id = self.table.data[idx]['id']
            WarehouseEditDialog(self, self.app, warehouse_id, self._load_data)


class WarehouseEditDialog(tk.Toplevel):
    """Діалог редагування складу"""
    
    def __init__(self, parent, app, warehouse_id: str = None, on_save=None):
        super().__init__(parent)
        self.app = app
        self.warehouse_id = warehouse_id
        self.on_save = on_save
        self.warehouse_dal = WarehouseDAL()
        
        self.title("Картка складу" if warehouse_id else "Новий склад")
        self.geometry("400x250")
        self.transient(parent)
        self.grab_set()
        
        self._create_widgets()
        if warehouse_id:
            self._load_data()
    
    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(fill='both', expand=True)
        
        ttk.Label(main_frame, text="Код:").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.code_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.code_var, width=20).grid(row=0, column=1, sticky='w', padx=5, pady=5)
        
        ttk.Label(main_frame, text="Найменування:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.name_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.name_var, width=40).grid(row=1, column=1, sticky='w', padx=5, pady=5)
        
        ttk.Label(main_frame, text="Адреса:").grid(row=2, column=0, sticky='e', padx=5, pady=5)
        self.address_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.address_var, width=40).grid(row=2, column=1, sticky='w', padx=5, pady=5)
        
        self.is_active_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(main_frame, text="Активний", variable=self.is_active_var).grid(
            row=3, column=1, sticky='w', padx=5, pady=5)
        
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(btn_frame, text="Зберегти", command=self._save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Закрити", command=self.destroy).pack(side='right', padx=5)
    
    def _load_data(self):
        warehouse = self.warehouse_dal.get_by_id(self.warehouse_id)
        if not warehouse:
            return
        
        self.code_var.set(warehouse['code'])
        self.name_var.set(warehouse['name'])
        self.address_var.set(warehouse['address'] or '')
        self.is_active_var.set(bool(warehouse['is_active']))
    
    def _save(self):
        code = self.code_var.get().strip()
        name = self.name_var.get().strip()
        
        if not code or not name:
            messagebox.showerror("Помилка", "Заповніть обов'язкові поля")
            return
        
        try:
            data = {
                'code': code,
                'name': name,
                'address': self.address_var.get().strip() or None,
                'is_active': 1 if self.is_active_var.get() else 0,
            }
            
            if self.warehouse_id:
                self.warehouse_dal.update(self.warehouse_id, data)
            else:
                self.warehouse_dal.create(data)
            
            messagebox.showinfo("Інформація", "Збережено")
            if self.on_save:
                self.on_save()
            self.destroy()
        except Exception as e:
            messagebox.showerror("Помилка", str(e))


class ZoneListFrame(BaseFrame):
    """Список зон складу"""
    
    def __init__(self, parent, app):
        super().__init__(parent, app)
        self.zone_dal = ZoneDAL()
        self.warehouse_dal = WarehouseDAL()
        self._create_widgets()
        self._load_data()
    
    def _create_widgets(self):
        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', padx=5, pady=5)
        
        ttk.Button(toolbar, text="➕ Створити", command=self._create).pack(side='left', padx=2)
        ttk.Button(toolbar, text="✏️ Редагувати", command=self._edit).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🔄 Оновити", command=self._load_data).pack(side='left', padx=2)
        
        columns = [
            ('id', 'ID', 0),
            ('code', 'Код', 100),
            ('name', 'Найменування', 200),
            ('zone_type', 'Тип', 150),
            ('warehouse_name', 'Склад', 200),
        ]
        self.table = DataTable(self, columns, on_double_click=self._edit_selected)
        self.table.pack(fill='both', expand=True, padx=5, pady=5)
    
    def _load_data(self):
        data = self.zone_dal.get_all()
        self.table.load_data(data)
    
    def _create(self):
        if not self.auth.has_permission('zones_edit'):
            self.show_error("Недостатньо прав")
            return
        ZoneEditDialog(self, self.app, None, self._load_data)
    
    def _edit(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть зону")
            return
        self._edit_selected(selected)
    
    def _edit_selected(self, selected):
        if not self.auth.has_permission('zones_edit'):
            self.show_error("Недостатньо прав")
            return
        idx = self.table.get_selected_index()
        if idx >= 0:
            zone_id = self.table.data[idx]['id']
            ZoneEditDialog(self, self.app, zone_id, self._load_data)


class ZoneEditDialog(tk.Toplevel):
    """Діалог редагування зони"""
    
    def __init__(self, parent, app, zone_id: str = None, on_save=None):
        super().__init__(parent)
        self.app = app
        self.zone_id = zone_id
        self.on_save = on_save
        self.zone_dal = ZoneDAL()
        self.warehouse_dal = WarehouseDAL()
        
        self.title("Картка зони" if zone_id else "Нова зона")
        self.geometry("400x300")
        self.transient(parent)
        self.grab_set()
        
        self._create_widgets()
        if zone_id:
            self._load_data()
    
    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(fill='both', expand=True)
        
        # Склад
        ttk.Label(main_frame, text="Склад:").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.warehouse_var = tk.StringVar()
        warehouses = self.warehouse_dal.get_all()
        self.warehouses_map = {w['name']: w['id'] for w in warehouses}
        self.warehouse_combo = ttk.Combobox(main_frame, textvariable=self.warehouse_var,
                                            values=list(self.warehouses_map.keys()), state='readonly', width=27)
        self.warehouse_combo.grid(row=0, column=1, sticky='w', padx=5, pady=5)
        
        # Код
        ttk.Label(main_frame, text="Код:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.code_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.code_var, width=20).grid(row=1, column=1, sticky='w', padx=5, pady=5)
        
        # Найменування
        ttk.Label(main_frame, text="Найменування:").grid(row=2, column=0, sticky='e', padx=5, pady=5)
        self.name_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.name_var, width=30).grid(row=2, column=1, sticky='w', padx=5, pady=5)
        
        # Тип
        ttk.Label(main_frame, text="Тип зони:").grid(row=3, column=0, sticky='e', padx=5, pady=5)
        self.zone_type_var = tk.StringVar()
        zone_types = [zt.value for zt in ZoneType]
        ttk.Combobox(main_frame, textvariable=self.zone_type_var, values=zone_types,
                    state='readonly', width=27).grid(row=3, column=1, sticky='w', padx=5, pady=5)
        
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(btn_frame, text="Зберегти", command=self._save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Закрити", command=self.destroy).pack(side='right', padx=5)
    
    def _load_data(self):
        zone = self.zone_dal.get_by_id(self.zone_id)
        if not zone:
            return
        
        # Знаходимо склад
        for name, wid in self.warehouses_map.items():
            if wid == zone['warehouse_id']:
                self.warehouse_var.set(name)
                break
        
        self.code_var.set(zone['code'])
        self.name_var.set(zone['name'])
        self.zone_type_var.set(zone['zone_type'])
    
    def _save(self):
        warehouse_id = self.warehouses_map.get(self.warehouse_var.get())
        code = self.code_var.get().strip()
        name = self.name_var.get().strip()
        zone_type = self.zone_type_var.get()
        
        if not warehouse_id or not code or not name or not zone_type:
            messagebox.showerror("Помилка", "Заповніть всі поля")
            return
        
        try:
            data = {
                'warehouse_id': warehouse_id,
                'code': code,
                'name': name,
                'zone_type': zone_type,
            }
            
            if self.zone_id:
                self.zone_dal.update(self.zone_id, data)
            else:
                self.zone_dal.create(data)
            
            messagebox.showinfo("Інформація", "Збережено")
            if self.on_save:
                self.on_save()
            self.destroy()
        except Exception as e:
            messagebox.showerror("Помилка", str(e))


class LocationListFrame(BaseFrame):
    """Список комірок"""
    
    def __init__(self, parent, app):
        super().__init__(parent, app)
        self.location_dal = LocationDAL()
        self._create_widgets()
        self._load_data()
    
    def _create_widgets(self):
        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', padx=5, pady=5)
        
        ttk.Button(toolbar, text="➕ Створити", command=self._create).pack(side='left', padx=2)
        ttk.Button(toolbar, text="✏️ Редагувати", command=self._edit).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🔄 Оновити", command=self._load_data).pack(side='left', padx=2)
        
        columns = [
            ('id', 'ID', 0),
            ('code', 'Код', 120),
            ('location_type', 'Тип', 100),
            ('zone_code', 'Зона', 80),
            ('zone_name', 'Назва зони', 150),
            ('warehouse_code', 'Склад', 80),
            ('max_weight', 'Макс. вага', 80),
            ('max_pallets', 'Макс. палет', 80),
        ]
        self.table = DataTable(self, columns, on_double_click=self._edit_selected)
        self.table.pack(fill='both', expand=True, padx=5, pady=5)
    
    def _load_data(self):
        data = self.location_dal.get_all()
        self.table.load_data(data)
    
    def _create(self):
        if not self.auth.has_permission('locations_edit'):
            self.show_error("Недостатньо прав")
            return
        LocationEditDialog(self, self.app, None, self._load_data)
    
    def _edit(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть комірку")
            return
        self._edit_selected(selected)
    
    def _edit_selected(self, selected):
        if not self.auth.has_permission('locations_edit'):
            self.show_error("Недостатньо прав")
            return
        idx = self.table.get_selected_index()
        if idx >= 0:
            location_id = self.table.data[idx]['id']
            LocationEditDialog(self, self.app, location_id, self._load_data)


class LocationEditDialog(tk.Toplevel):
    """Діалог редагування комірки"""
    
    def __init__(self, parent, app, location_id: str = None, on_save=None):
        super().__init__(parent)
        self.app = app
        self.location_id = location_id
        self.on_save = on_save
        self.location_dal = LocationDAL()
        self.zone_dal = ZoneDAL()
        
        self.title("Картка комірки" if location_id else "Нова комірка")
        self.geometry("400x350")
        self.transient(parent)
        self.grab_set()
        
        self._create_widgets()
        if location_id:
            self._load_data()
    
    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(fill='both', expand=True)
        
        # Зона
        ttk.Label(main_frame, text="Зона:").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.zone_var = tk.StringVar()
        zones = self.zone_dal.get_all()
        self.zones_map = {f"{z['code']} - {z['name']}": z['id'] for z in zones}
        self.zone_combo = ttk.Combobox(main_frame, textvariable=self.zone_var,
                                       values=list(self.zones_map.keys()), state='readonly', width=27)
        self.zone_combo.grid(row=0, column=1, sticky='w', padx=5, pady=5)
        
        # Код
        ttk.Label(main_frame, text="Код:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.code_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.code_var, width=20).grid(row=1, column=1, sticky='w', padx=5, pady=5)
        
        # Тип
        ttk.Label(main_frame, text="Тип:").grid(row=2, column=0, sticky='e', padx=5, pady=5)
        self.location_type_var = tk.StringVar()
        loc_types = [lt.value for lt in LocationType]
        ttk.Combobox(main_frame, textvariable=self.location_type_var, values=loc_types,
                    state='readonly', width=17).grid(row=2, column=1, sticky='w', padx=5, pady=5)
        
        # Макс. вага
        ttk.Label(main_frame, text="Макс. вага (кг):").grid(row=3, column=0, sticky='e', padx=5, pady=5)
        self.max_weight_var = tk.StringVar(value="0")
        ttk.Entry(main_frame, textvariable=self.max_weight_var, width=10).grid(row=3, column=1, sticky='w', padx=5, pady=5)
        
        # Макс. об'єм
        ttk.Label(main_frame, text="Макс. об'єм (м³):").grid(row=4, column=0, sticky='e', padx=5, pady=5)
        self.max_volume_var = tk.StringVar(value="0")
        ttk.Entry(main_frame, textvariable=self.max_volume_var, width=10).grid(row=4, column=1, sticky='w', padx=5, pady=5)
        
        # Макс. палет
        ttk.Label(main_frame, text="Макс. палет:").grid(row=5, column=0, sticky='e', padx=5, pady=5)
        self.max_pallets_var = tk.StringVar(value="1")
        ttk.Entry(main_frame, textvariable=self.max_pallets_var, width=10).grid(row=5, column=1, sticky='w', padx=5, pady=5)
        
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(btn_frame, text="Зберегти", command=self._save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Закрити", command=self.destroy).pack(side='right', padx=5)
    
    def _load_data(self):
        location = self.location_dal.get_by_id(self.location_id)
        if not location:
            return
        
        # Знаходимо зону
        zone = self.zone_dal.get_by_id(location['zone_id'])
        if zone:
            for name, zid in self.zones_map.items():
                if zid == zone['id']:
                    self.zone_var.set(name)
                    break
        
        self.code_var.set(location['code'])
        self.location_type_var.set(location['location_type'])
        self.max_weight_var.set(str(location['max_weight'] or 0))
        self.max_volume_var.set(str(location['max_volume'] or 0))
        self.max_pallets_var.set(str(location['max_pallets'] or 1))
    
    def _save(self):
        zone_id = self.zones_map.get(self.zone_var.get())
        code = self.code_var.get().strip()
        location_type = self.location_type_var.get()
        
        if not zone_id or not code or not location_type:
            messagebox.showerror("Помилка", "Заповніть обов'язкові поля")
            return
        
        try:
            data = {
                'zone_id': zone_id,
                'code': code,
                'location_type': location_type,
                'max_weight': float(self.max_weight_var.get() or 0),
                'max_volume': float(self.max_volume_var.get() or 0),
                'max_pallets': int(self.max_pallets_var.get() or 1),
            }
            
            if self.location_id:
                self.location_dal.update(self.location_id, data)
            else:
                self.location_dal.create(data)
            
            messagebox.showinfo("Інформація", "Збережено")
            if self.on_save:
                self.on_save()
            self.destroy()
        except Exception as e:
            messagebox.showerror("Помилка", str(e))


# ============================================================================
# ФОРМИ ДОКУМЕНТІВ
# ============================================================================

class InboundOrderListFrame(BaseFrame):
    """Список замовлень на приймання"""
    
    def __init__(self, parent, app):
        super().__init__(parent, app)
        self.inbound_dal = InboundOrderDAL()
        self.inbound_service = InboundService(
            app.auth_service,
            InventoryService(app.auth_service)
        )
        self._create_widgets()
        self._load_data()
    
    def _create_widgets(self):
        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', padx=5, pady=5)
        
        ttk.Button(toolbar, text="➕ Створити", command=self._create).pack(side='left', padx=2)
        ttk.Button(toolbar, text="✏️ Редагувати", command=self._edit).pack(side='left', padx=2)
        ttk.Button(toolbar, text="✅ Провести", command=self._post).pack(side='left', padx=2)
        ttk.Button(toolbar, text="❌ Скасувати", command=self._cancel).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🗑️ Видалити", command=self._delete).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🔄 Оновити", command=self._load_data).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🖨️ Друк", command=self._print).pack(side='left', padx=2)
        
        # Фільтри
        filter_frame = ttk.Frame(self)
        filter_frame.pack(fill='x', padx=5, pady=2)
        
        ttk.Label(filter_frame, text="Статус:").pack(side='left', padx=5)
        self.status_var = tk.StringVar()
        statuses = ['', DocStatus.DRAFT.value, DocStatus.IN_PROGRESS.value, 
                   DocStatus.POSTED.value, DocStatus.CANCELLED.value]
        ttk.Combobox(filter_frame, textvariable=self.status_var, values=statuses,
                    state='readonly', width=15).pack(side='left', padx=5)
        
        ttk.Button(filter_frame, text="Фільтр", command=self._load_data).pack(side='left', padx=5)
        
        columns = [
            ('id', 'ID', 0),
            ('doc_number', '№ документа', 150),
            ('doc_date', 'Дата', 100),
            ('status', 'Статус', 100),
            ('client_name', 'Клієнт', 150),
            ('supplier_name', 'Постачальник', 150),
            ('warehouse_name', 'Склад', 120),
            ('expected_date', 'Очікувана дата', 100),
        ]
        self.table = DataTable(self, columns, on_double_click=self._edit_selected)
        self.table.pack(fill='both', expand=True, padx=5, pady=5)
    
    def _load_data(self):
        status = self.status_var.get() if hasattr(self, 'status_var') else None
        data = self.inbound_dal.get_all(status=status if status else None)
        self.table.load_data(data)
    
    def _create(self):
        if not self.auth.has_permission('inbound_edit'):
            self.show_error("Недостатньо прав")
            return
        InboundOrderEditDialog(self, self.app, None, self._load_data)
    
    def _edit(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        self._edit_selected(selected)
    
    def _edit_selected(self, selected):
        if not self.auth.has_permission('inbound_edit'):
            self.show_error("Недостатньо прав")
            return
        idx = self.table.get_selected_index()
        if idx >= 0:
            order_id = self.table.data[idx]['id']
            InboundOrderEditDialog(self, self.app, order_id, self._load_data)
    
    def _post(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        
        if not self.ask_confirm("Провести документ?"):
            return
        
        try:
            idx = self.table.get_selected_index()
            order_id = self.table.data[idx]['id']
            self.inbound_service.post_order(order_id)
            self.show_info("Документ проведено")
            self._load_data()
        except Exception as e:
            self.show_error(str(e))
    
    def _cancel(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        
        if not self.ask_confirm("Скасувати проведення?"):
            return
        
        try:
            idx = self.table.get_selected_index()
            order_id = self.table.data[idx]['id']
            self.inbound_service.cancel_posting(order_id)
            self.show_info("Проведення скасовано")
            self._load_data()
        except Exception as e:
            self.show_error(str(e))
    
    def _delete(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        
        if not self.ask_confirm("Видалити документ?"):
            return
        
        try:
            idx = self.table.get_selected_index()
            order_id = self.table.data[idx]['id']
            self.inbound_dal.delete(order_id)
            self.show_info("Документ видалено")
            self._load_data()
        except Exception as e:
            self.show_error(str(e))
    
    def _print(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        
        try:
            idx = self.table.get_selected_index()
            order_id = self.table.data[idx]['id']
            PrintService().print_inbound_act(order_id)
        except Exception as e:
            self.show_error(str(e))


class InboundOrderEditDialog(tk.Toplevel):
    """Діалог редагування замовлення на приймання"""
    
    def __init__(self, parent, app, order_id: str = None, on_save=None):
        super().__init__(parent)
        self.app = app
        self.order_id = order_id
        self.on_save = on_save
        self.inbound_dal = InboundOrderDAL()
        self.client_dal = ClientDAL()
        self.supplier_dal = SupplierDAL()
        self.warehouse_dal = WarehouseDAL()
        self.item_dal = ItemDAL()
        
        self.title("Замовлення на приймання" if order_id else "Нове замовлення на приймання")
        self.geometry("900x600")
        self.transient(parent)
        self.grab_set()
        
        self.lines_data = []
        self._create_widgets()
        if order_id:
            self._load_data()
    
    def _create_widgets(self):
        # Заголовок
        header_frame = ttk.LabelFrame(self, text="Реквізити документа", padding=10)
        header_frame.pack(fill='x', padx=10, pady=5)
        
        # Клієнт
        ttk.Label(header_frame, text="Клієнт:").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.client_var = tk.StringVar()
        clients = self.client_dal.get_all()
        self.clients_map = {c['name']: c['id'] for c in clients}
        ttk.Combobox(header_frame, textvariable=self.client_var,
                    values=list(self.clients_map.keys()), state='readonly', width=30).grid(row=0, column=1, sticky='w', padx=5, pady=5)
        
        # Постачальник
        ttk.Label(header_frame, text="Постачальник:").grid(row=0, column=2, sticky='e', padx=5, pady=5)
        self.supplier_var = tk.StringVar()
        suppliers = self.supplier_dal.get_all()
        self.suppliers_map = {s['name']: s['id'] for s in suppliers}
        self.suppliers_map[''] = None
        ttk.Combobox(header_frame, textvariable=self.supplier_var,
                    values=[''] + list(self.suppliers_map.keys())[:-1], state='readonly', width=30).grid(row=0, column=3, sticky='w', padx=5, pady=5)
        
        # Склад
        ttk.Label(header_frame, text="Склад:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.warehouse_var = tk.StringVar()
        warehouses = self.warehouse_dal.get_all()
        self.warehouses_map = {w['name']: w['id'] for w in warehouses}
        ttk.Combobox(header_frame, textvariable=self.warehouse_var,
                    values=list(self.warehouses_map.keys()), state='readonly', width=30).grid(row=1, column=1, sticky='w', padx=5, pady=5)
        
        # Дата
        ttk.Label(header_frame, text="Дата:").grid(row=1, column=2, sticky='e', padx=5, pady=5)
        self.date_var = tk.StringVar(value=today_str())
        ttk.Entry(header_frame, textvariable=self.date_var, width=15).grid(row=1, column=3, sticky='w', padx=5, pady=5)
        
        # Очікувана дата
        ttk.Label(header_frame, text="Очікувана дата:").grid(row=2, column=0, sticky='e', padx=5, pady=5)
        self.expected_date_var = tk.StringVar()
        ttk.Entry(header_frame, textvariable=self.expected_date_var, width=15).grid(row=2, column=1, sticky='w', padx=5, pady=5)
        
        # Примітки
        ttk.Label(header_frame, text="Примітки:").grid(row=2, column=2, sticky='e', padx=5, pady=5)
        self.notes_var = tk.StringVar()
        ttk.Entry(header_frame, textvariable=self.notes_var, width=40).grid(row=2, column=3, sticky='w', padx=5, pady=5)
        
        # Табличнечна частина
        lines_frame = ttk.LabelFrame(self, text="Позиції", padding=10)
        lines_frame.pack(fill='both', expand=True, padx=10, pady=5)
                # Кнопки для позицій
        lines_toolbar = ttk.Frame(lines_frame)
        lines_toolbar.pack(fill='x', pady=5)
        
        ttk.Button(lines_toolbar, text="➕ Додати", command=self._add_line).pack(side='left', padx=2)
        ttk.Button(lines_toolbar, text="✏️ Редагувати", command=self._edit_line).pack(side='left', padx=2)
        ttk.Button(lines_toolbar, text="🗑️ Видалити", command=self._remove_line).pack(side='left', padx=2)
        
        # Таблиця позицій
        columns = [
            ('line_number', '№', 40),
            ('sku', 'Артикул', 100),
            ('item_name', 'Найменування', 200),
            ('expected_qty', 'Очікувана к-сть', 100),
            ('received_qty', 'Прийнята к-сть', 100),
            ('batch_number', 'Партія', 100),
            ('expiry_date', 'Термін придатності', 120),
        ]
        self.lines_table = DataTable(lines_frame, columns)
        self.lines_table.pack(fill='both', expand=True)
        
        # Кнопки
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(btn_frame, text="💾 Зберегти", command=self._save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="💾 Зберегти і закрити", command=self._save_and_close).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="❌ Закрити", command=self.destroy).pack(side='right', padx=5)
    
    def _load_data(self):
        order = self.inbound_dal.get_by_id(self.order_id)
        if not order:
            return
        
        # Клієнт
        if order['client_name']:
            self.client_var.set(order['client_name'])
        
        # Постачальник
        if order['supplier_name']:
            self.supplier_var.set(order['supplier_name'])
        
        # Склад
        if order['warehouse_name']:
            self.warehouse_var.set(order['warehouse_name'])
        
        self.date_var.set(order['doc_date'])
        self.expected_date_var.set(order['expected_date'] or '')
        self.notes_var.set(order['notes'] or '')
        
        # Позиції
        self.lines_data = self.inbound_dal.get_lines(self.order_id)
        self._refresh_lines_table()
    
    def _refresh_lines_table(self):
        display_data = []
        for idx, line in enumerate(self.lines_data, 1):
            display_data.append({
                'line_number': idx,
                'sku': line.get('sku', ''),
                'item_name': line.get('item_name', ''),
                'expected_qty': line.get('expected_qty', 0),
                'received_qty': line.get('received_qty', 0),
                'batch_number': line.get('batch_number', ''),
                'expiry_date': line.get('expiry_date', ''),
            })
        self.lines_table.load_data(display_data)
    
    def _add_line(self):
        dialog = InboundLineEditDialog(self, self.app, None)
        self.wait_window(dialog)
        if hasattr(dialog, 'result') and dialog.result:
            self.lines_data.append(dialog.result)
            self._refresh_lines_table()
    
    def _edit_line(self):
        idx = self.lines_table.get_selected_index()
        if idx < 0:
            messagebox.showwarning("Увага", "Виберіть позицію")
            return
        
        line = self.lines_data[idx]
        dialog = InboundLineEditDialog(self, self.app, line)
        self.wait_window(dialog)
        if hasattr(dialog, 'result') and dialog.result:
            self.lines_data[idx] = dialog.result
            self._refresh_lines_table()
    
    def _remove_line(self):
        idx = self.lines_table.get_selected_index()
        if idx < 0:
            messagebox.showwarning("Увага", "Виберіть позицію")
            return
        
        if messagebox.askyesno("Підтвердження", "Видалити позицію?"):
            del self.lines_data[idx]
            self._refresh_lines_table()
    
    def _get_data(self) -> dict:
        return {
            'client_id': self.clients_map.get(self.client_var.get()),
            'supplier_id': self.suppliers_map.get(self.supplier_var.get()),
            'warehouse_id': self.warehouses_map.get(self.warehouse_var.get()),
            'doc_date': self.date_var.get(),
            'expected_date': self.expected_date_var.get() or None,
            'notes': self.notes_var.get() or None,
        }
    
    def _validate(self) -> bool:
        data = self._get_data()
        if not data['client_id']:
            messagebox.showerror("Помилка", "Виберіть клієнта")
            return False
        if not data['warehouse_id']:
            messagebox.showerror("Помилка", "Виберіть склад")
            return False
        if not self.lines_data:
            messagebox.showerror("Помилка", "Додайте позиції")
            return False
        return True
    
    def _save(self) -> bool:
        if not self._validate():
            return False
        
        try:
            data = self._get_data()
            lines = [{
                'item_id': line['item_id'],
                'expected_qty': line['expected_qty'],
                'received_qty': line.get('received_qty', 0),
                'batch_number': line.get('batch_number'),
                'expiry_date': line.get('expiry_date'),
                'notes': line.get('notes'),
            } for line in self.lines_data]
            
            if self.order_id:
                self.inbound_dal.update(self.order_id, data, lines)
            else:
                user_id = self.app.auth_service.current_user['id'] if self.app.auth_service.current_user else None
                self.order_id = self.inbound_dal.create(data, lines, user_id)
            
            messagebox.showinfo("Інформація", "Збережено")
            if self.on_save:
                self.on_save()
            return True
        except Exception as e:
            messagebox.showerror("Помилка", str(e))
            return False
    
    def _save_and_close(self):
        if self._save():
            self.destroy()


class InboundLineEditDialog(tk.Toplevel):
    """Діалог редагування позиції замовлення на приймання"""
    
    def __init__(self, parent, app, line_data: dict = None):
        super().__init__(parent)
        self.app = app
        self.line_data = line_data
        self.item_dal = ItemDAL()
        self.result = None
        
        self.title("Позиція замовлення")
        self.geometry("450x350")
        self.transient(parent)
        self.grab_set()
        
        self._create_widgets()
        if line_data:
            self._load_data()
    
    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(fill='both', expand=True)
        
        # Товар
        ttk.Label(main_frame, text="Товар:").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.item_var = tk.StringVar()
        items = self.item_dal.get_all()
        self.items_map = {f"{i['sku']} - {i['name']}": i for i in items}
        self.item_combo = ttk.Combobox(main_frame, textvariable=self.item_var,
                                       values=list(self.items_map.keys()), width=40)
        self.item_combo.grid(row=0, column=1, sticky='w', padx=5, pady=5)
        
        # Очікувана кількість
        ttk.Label(main_frame, text="Очікувана кількість:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.expected_qty_var = tk.StringVar(value="0")
        ttk.Entry(main_frame, textvariable=self.expected_qty_var, width=15).grid(row=1, column=1, sticky='w', padx=5, pady=5)
        
        # Прийнята кількість
        ttk.Label(main_frame, text="Прийнята кількість:").grid(row=2, column=0, sticky='e', padx=5, pady=5)
        self.received_qty_var = tk.StringVar(value="0")
        ttk.Entry(main_frame, textvariable=self.received_qty_var, width=15).grid(row=2, column=1, sticky='w', padx=5, pady=5)
        
        # Партія
        ttk.Label(main_frame, text="Партія:").grid(row=3, column=0, sticky='e', padx=5, pady=5)
        self.batch_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.batch_var, width=20).grid(row=3, column=1, sticky='w', padx=5, pady=5)
        
        # Термін придатності
        ttk.Label(main_frame, text="Термін придатності:").grid(row=4, column=0, sticky='e', padx=5, pady=5)
        self.expiry_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.expiry_var, width=15).grid(row=4, column=1, sticky='w', padx=5, pady=5)
        ttk.Label(main_frame, text="(YYYY-MM-DD)").grid(row=4, column=2, sticky='w')
        
        # Примітки
        ttk.Label(main_frame, text="Примітки:").grid(row=5, column=0, sticky='e', padx=5, pady=5)
        self.notes_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.notes_var, width=40).grid(row=5, column=1, sticky='w', padx=5, pady=5)
        
        # Кнопки
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(btn_frame, text="OK", command=self._ok).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Скасувати", command=self.destroy).pack(side='right', padx=5)
    
    def _load_data(self):
        # Знаходимо товар
        for name, item in self.items_map.items():
            if item['id'] == self.line_data.get('item_id'):
                self.item_var.set(name)
                break
        
        self.expected_qty_var.set(str(self.line_data.get('expected_qty', 0)))
        self.received_qty_var.set(str(self.line_data.get('received_qty', 0)))
        self.batch_var.set(self.line_data.get('batch_number', ''))
        self.expiry_var.set(self.line_data.get('expiry_date', ''))
        self.notes_var.set(self.line_data.get('notes', ''))
    
    def _ok(self):
        item = self.items_map.get(self.item_var.get())
        if not item:
            messagebox.showerror("Помилка", "Виберіть товар")
            return
        
        try:
            expected_qty = float(self.expected_qty_var.get() or 0)
            received_qty = float(self.received_qty_var.get() or 0)
        except ValueError:
            messagebox.showerror("Помилка", "Невірна кількість")
            return
        
        if expected_qty <= 0:
            messagebox.showerror("Помилка", "Кількість повинна бути більше 0")
            return
        
        self.result = {
            'item_id': item['id'],
            'sku': item['sku'],
            'item_name': item['name'],
            'expected_qty': expected_qty,
            'received_qty': received_qty,
            'batch_number': self.batch_var.get() or None,
            'expiry_date': self.expiry_var.get() or None,
            'notes': self.notes_var.get() or None,
        }
        self.destroy()


class OutboundOrderListFrame(BaseFrame):
    """Список замовлень на відвантаження"""
    
    def __init__(self, parent, app):
        super().__init__(parent, app)
        self.outbound_dal = OutboundOrderDAL()
        self.outbound_service = OutboundService(
            app.auth_service,
            InventoryService(app.auth_service)
        )
        self._create_widgets()
        self._load_data()
    
    def _create_widgets(self):
        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', padx=5, pady=5)
        
        ttk.Button(toolbar, text="➕ Створити", command=self._create).pack(side='left', padx=2)
        ttk.Button(toolbar, text="✏️ Редагувати", command=self._edit).pack(side='left', padx=2)
        ttk.Button(toolbar, text="📦 Резервувати", command=self._reserve).pack(side='left', padx=2)
        ttk.Button(toolbar, text="✅ Відвантажити", command=self._post).pack(side='left', padx=2)
        ttk.Button(toolbar, text="❌ Скасувати", command=self._cancel).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🗑️ Видалити", command=self._delete).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🔄 Оновити", command=self._load_data).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🖨️ Накладна", command=self._print_invoice).pack(side='left', padx=2)
        ttk.Button(toolbar, text="📋 Лист збору", command=self._print_picklist).pack(side='left', padx=2)
        
        # Фільтри
        filter_frame = ttk.Frame(self)
        filter_frame.pack(fill='x', padx=5, pady=2)
        
        ttk.Label(filter_frame, text="Статус:").pack(side='left', padx=5)
        self.status_var = tk.StringVar()
        statuses = ['', DocStatus.DRAFT.value, DocStatus.IN_PROGRESS.value,
                   DocStatus.POSTED.value, DocStatus.CANCELLED.value]
        ttk.Combobox(filter_frame, textvariable=self.status_var, values=statuses,
                    state='readonly', width=15).pack(side='left', padx=5)
        
        ttk.Button(filter_frame, text="Фільтр", command=self._load_data).pack(side='left', padx=5)
        
        columns = [
            ('id', 'ID', 0),
            ('doc_number', '№ документа', 150),
            ('doc_date', 'Дата', 100),
            ('status', 'Статус', 100),
            ('client_name', 'Клієнт', 150),
            ('warehouse_name', 'Склад', 120),
            ('carrier_name', 'Перевізник', 120),
            ('delivery_date', 'Дата доставки', 100),
            ('priority', 'Пріоритет', 70),
        ]
        self.table = DataTable(self, columns, on_double_click=self._edit_selected)
        self.table.pack(fill='both', expand=True, padx=5, pady=5)
    
    def _load_data(self):
        status = self.status_var.get() if hasattr(self, 'status_var') else None
        data = self.outbound_dal.get_all(status=status if status else None)
        self.table.load_data(data)
    
    def _create(self):
        if not self.auth.has_permission('outbound_edit'):
            self.show_error("Недостатньо прав")
            return
        OutboundOrderEditDialog(self, self.app, None, self._load_data)
    
    def _edit(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        self._edit_selected(selected)
    
    def _edit_selected(self, selected):
        if not self.auth.has_permission('outbound_edit'):
            self.show_error("Недостатньо прав")
            return
        idx = self.table.get_selected_index()
        if idx >= 0:
            order_id = self.table.data[idx]['id']
            OutboundOrderEditDialog(self, self.app, order_id, self._load_data)
    
    def _reserve(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        
        if not self.ask_confirm("Резервувати товар?"):
            return
        
        try:
            idx = self.table.get_selected_index()
            order_id = self.table.data[idx]['id']
            self.outbound_service.reserve_order(order_id)
            self.show_info("Товар зарезервовано")
            self._load_data()
        except Exception as e:
            self.show_error(str(e))
    
    def _post(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        
        if not self.ask_confirm("Відвантажити замовлення?"):
            return
        
        try:
            idx = self.table.get_selected_index()
            order_id = self.table.data[idx]['id']
            self.outbound_service.post_order(order_id)
            self.show_info("Замовлення відвантажено")
            self._load_data()
        except Exception as e:
            self.show_error(str(e))
    
    def _cancel(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        
        if not self.ask_confirm("Скасувати відвантаження?"):
            return
        
        try:
            idx = self.table.get_selected_index()
            order_id = self.table.data[idx]['id']
            self.outbound_service.cancel_posting(order_id)
            self.show_info("Відвантаження скасовано")
            self._load_data()
        except Exception as e:
            self.show_error(str(e))
    
    def _delete(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        
        if not self.ask_confirm("Видалити документ?"):
            return
        
        try:
            idx = self.table.get_selected_index()
            order_id = self.table.data[idx]['id']
            self.outbound_dal.delete(order_id)
            self.show_info("Документ видалено")
            self._load_data()
        except Exception as e:
            self.show_error(str(e))
    
    def _print_invoice(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        
        try:
            idx = self.table.get_selected_index()
            order_id = self.table.data[idx]['id']
            PrintService().print_outbound_invoice(order_id)
        except Exception as e:
            self.show_error(str(e))
    
    def _print_picklist(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        
        try:
            idx = self.table.get_selected_index()
            order_id = self.table.data[idx]['id']
            PrintService().print_outbound_picklist(order_id)
        except Exception as e:
            self.show_error(str(e))


class OutboundOrderEditDialog(tk.Toplevel):
    """Діалог редагування замовлення на відвантаження"""
    
    def __init__(self, parent, app, order_id: str = None, on_save=None):
        super().__init__(parent)
        self.app = app
        self.order_id = order_id
        self.on_save = on_save
        self.outbound_dal = OutboundOrderDAL()
        self.client_dal = ClientDAL()
        self.warehouse_dal = WarehouseDAL()
        self.carrier_dal = CarrierDAL()
        self.item_dal = ItemDAL()
        
        self.title("Замовлення на відвантаження" if order_id else "Нове замовлення на відвантаження")
        self.geometry("900x600")
        self.transient(parent)
        self.grab_set()
        
        self.lines_data = []
        self._create_widgets()
        if order_id:
            self._load_data()
    
    def _create_widgets(self):
        # Заголовок
        header_frame = ttk.LabelFrame(self, text="Реквізити документа", padding=10)
        header_frame.pack(fill='x', padx=10, pady=5)
        
        # Клієнт
        ttk.Label(header_frame, text="Клієнт:").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.client_var = tk.StringVar()
        clients = self.client_dal.get_all()
        self.clients_map = {c['name']: c['id'] for c in clients}
        ttk.Combobox(header_frame, textvariable=self.client_var,
                    values=list(self.clients_map.keys()), state='readonly', width=30).grid(row=0, column=1, sticky='w', padx=5, pady=5)
        
        # Склад
        ttk.Label(header_frame, text="Склад:").grid(row=0, column=2, sticky='e', padx=5, pady=5)
        self.warehouse_var = tk.StringVar()
        warehouses = self.warehouse_dal.get_all()
        self.warehouses_map = {w['name']: w['id'] for w in warehouses}
        ttk.Combobox(header_frame, textvariable=self.warehouse_var,
                    values=list(self.warehouses_map.keys()), state='readonly', width=30).grid(row=0, column=3, sticky='w', padx=5, pady=5)
        
        # Перевізник
        ttk.Label(header_frame, text="Перевізник:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.carrier_var = tk.StringVar()
        carriers = self.carrier_dal.get_all()
        self.carriers_map = {c['name']: c['id'] for c in carriers}
        self.carriers_map[''] = None
        ttk.Combobox(header_frame, textvariable=self.carrier_var,
                    values=[''] + list(self.carriers_map.keys())[:-1], state='readonly', width=30).grid(row=1, column=1, sticky='w', padx=5, pady=5)
        
        # Дата
        ttk.Label(header_frame, text="Дата:").grid(row=1, column=2, sticky='e', padx=5, pady=5)
        self.date_var = tk.StringVar(value=today_str())
        ttk.Entry(header_frame, textvariable=self.date_var, width=15).grid(row=1, column=3, sticky='w', padx=5, pady=5)
        
        # Адреса доставки
        ttk.Label(header_frame, text="Адреса доставки:").grid(row=2, column=0, sticky='e', padx=5, pady=5)
        self.address_var = tk.StringVar()
        ttk.Entry(header_frame, textvariable=self.address_var, width=50).grid(row=2, column=1, columnspan=2, sticky='w', padx=5, pady=5)
        
        # Дата доставки
        ttk.Label(header_frame, text="Дата доставки:").grid(row=2, column=3, sticky='e', padx=5, pady=5)
        self.delivery_date_var = tk.StringVar()
        ttk.Entry(header_frame, textvariable=self.delivery_date_var, width=15).grid(row=2, column=4, sticky='w', padx=5, pady=5)
        
        # Пріоритет
        ttk.Label(header_frame, text="Пріоритет:").grid(row=3, column=0, sticky='e', padx=5, pady=5)
        self.priority_var = tk.StringVar(value="5")
        ttk.Combobox(header_frame, textvariable=self.priority_var,
                    values=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'], width=10).grid(row=3, column=1, sticky='w', padx=5, pady=5)
        
        # Примітки
        ttk.Label(header_frame, text="Примітки:").grid(row=3, column=2, sticky='e', padx=5, pady=5)
        self.notes_var = tk.StringVar()
        ttk.Entry(header_frame, textvariable=self.notes_var, width=40).grid(row=3, column=3, sticky='w', padx=5, pady=5)
        
        # Таблична частина
        lines_frame = ttk.LabelFrame(self, text="Позиції", padding=10)
        lines_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        # Кнопки для позицій
        lines_toolbar = ttk.Frame(lines_frame)
        lines_toolbar.pack(fill='x', pady=5)
        
        ttk.Button(lines_toolbar, text="➕ Додати", command=self._add_line).pack(side='left', padx=2)
        ttk.Button(lines_toolbar, text="✏️ Редагувати", command=self._edit_line).pack(side='left', padx=2)
        ttk.Button(lines_toolbar, text="🗑️ Видалити", command=self._remove_line).pack(side='left', padx=2)
        
        # Таблиця позицій
        columns = [
            ('line_number', '№', 40),
            ('sku', 'Артикул', 100),
            ('item_name', 'Найменування', 200),
            ('ordered_qty', 'Замовлено', 100),
            ('reserved_qty', 'Зарезервовано', 100),
            ('shipped_qty', 'Відвантажено', 100),
        ]
        self.lines_table = DataTable(lines_frame, columns)
        self.lines_table.pack(fill='both', expand=True)
        
        # Кнопки
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(btn_frame, text="💾 Зберегти", command=self._save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="💾 Зберегти і закрити", command=self._save_and_close).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="❌ Закрити", command=self.destroy).pack(side='right', padx=5)
    
    def _load_data(self):
        order = self.outbound_dal.get_by_id(self.order_id)
        if not order:
            return
        
        if order['client_name']:
            self.client_var.set(order['client_name'])
        if order['warehouse_name']:
            self.warehouse_var.set(order['warehouse_name'])
        if order['carrier_name']:
            self.carrier_var.set(order['carrier_name'])
        
        self.date_var.set(order['doc_date'])
        self.address_var.set(order['delivery_address'] or '')
        self.delivery_date_var.set(order['delivery_date'] or '')
        self.priority_var.set(str(order['priority'] or 5))
        self.notes_var.set(order['notes'] or '')
        
        self.lines_data = self.outbound_dal.get_lines(self.order_id)
        self._refresh_lines_table()
    
    def _refresh_lines_table(self):
        display_data = []
        for idx, line in enumerate(self.lines_data, 1):
            display_data.append({
                'line_number': idx,
                'sku': line.get('sku', ''),
                'item_name': line.get('item_name', ''),
                'ordered_qty': line.get('ordered_qty', 0),
                'reserved_qty': line.get('reserved_qty', 0),
                'shipped_qty': line.get('shipped_qty', 0),
            })
        self.lines_table.load_data(display_data)
    
    def _add_line(self):
        dialog = OutboundLineEditDialog(self, self.app, None)
        self.wait_window(dialog)
        if hasattr(dialog, 'result') and dialog.result:
            self.lines_data.append(dialog.result)
            self._refresh_lines_table()
    
    def _edit_line(self):
        idx = self.lines_table.get_selected_index()
        if idx < 0:
            messagebox.showwarning("Увага", "Виберіть позицію")
            return
        
        line = self.lines_data[idx]
        dialog = OutboundLineEditDialog(self, self.app, line)
        self.wait_window(dialog)
        if hasattr(dialog, 'result') and dialog.result:
            self.lines_data[idx] = dialog.result
            self._refresh_lines_table()
    
    def _remove_line(self):
        idx = self.lines_table.get_selected_index()
        if idx < 0:
            messagebox.showwarning("Увага", "Виберіть позицію")
            return
        
        if messagebox.askyesno("Підтвердження", "Видалити позицію?"):
            del self.lines_data[idx]
            self._refresh_lines_table()
    
    def _get_data(self) -> dict:
        return {
            'client_id': self.clients_map.get(self.client_var.get()),
            'warehouse_id': self.warehouses_map.get(self.warehouse_var.get()),
            'carrier_id': self.carriers_map.get(self.carrier_var.get()),
            'doc_date': self.date_var.get(),
            'delivery_address': self.address_var.get() or None,
            'delivery_date': self.delivery_date_var.get() or None,
            'priority': int(self.priority_var.get() or 5),
            'notes': self.notes_var.get() or None,
        }
    
    def _validate(self) -> bool:
        data = self._get_data()
        if not data['client_id']:
            messagebox.showerror("Помилка", "Виберіть клієнта")
            return False
        if not data['warehouse_id']:
            messagebox.showerror("Помилка", "Виберіть склад")
            return False
        if not self.lines_data:
            messagebox.showerror("Помилка", "Додайте позиції")
            return False
        return True
    
    def _save(self) -> bool:
        if not self._validate():
            return False
        
        try:
            data = self._get_data()
            lines = [{
                'item_id': line['item_id'],
                'ordered_qty': line['ordered_qty'],
                'reserved_qty': line.get('reserved_qty', 0),
                'picked_qty': line.get('picked_qty', 0),
                'shipped_qty': line.get('shipped_qty', 0),
                'batch_number': line.get('batch_number'),
                'expiry_date': line.get('expiry_date'),
                'notes': line.get('notes'),
            } for line in self.lines_data]
            
            if self.order_id:
                self.outbound_dal.update(self.order_id, data, lines)
            else:
                user_id = self.app.auth_service.current_user['id'] if self.app.auth_service.current_user else None
                self.order_id = self.outbound_dal.create(data, lines, user_id)
            
            messagebox.showinfo("Інформація", "Збережено")
            if self.on_save:
                self.on_save()
            return True
        except Exception as e:
            messagebox.showerror("Помилка", str(e))
            return False
    
    def _save_and_close(self):
        if self._save():
            self.destroy()


class OutboundLineEditDialog(tk.Toplevel):
    """Діалог редагування позиції замовлення на відвантаження"""
    
    def __init__(self, parent, app, line_data: dict = None):
        super().__init__(parent)
        self.app = app
        self.line_data = line_data
        self.item_dal = ItemDAL()
        self.result = None
        
        self.title("Позиція замовлення")
        self.geometry("450x250")
        self.transient(parent)
        self.grab_set()
        
        self._create_widgets()
        if line_data:
            self._load_data()
    
    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(fill='both', expand=True)
        
        # Товар
        ttk.Label(main_frame, text="Товар:").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.item_var = tk.StringVar()
        items = self.item_dal.get_all()
        self.items_map = {f"{i['sku']} - {i['name']}": i for i in items}
        self.item_combo = ttk.Combobox(main_frame, textvariable=self.item_var,
                                       values=list(self.items_map.keys()), width=40)
        self.item_combo.grid(row=0, column=1, sticky='w', padx=5, pady=5)
        
        # Кількість
        ttk.Label(main_frame, text="Кількість:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.qty_var = tk.StringVar(value="0")
        ttk.Entry(main_frame, textvariable=self.qty_var, width=15).grid(row=1, column=1, sticky='w', padx=5, pady=5)
        
        # Примітки
        ttk.Label(main_frame, text="Примітки:").grid(row=2, column=0, sticky='e', padx=5, pady=5)
        self.notes_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.notes_var, width=40).grid(row=2, column=1, sticky='w', padx=5, pady=5)
        
        # Кнопки
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(btn_frame, text="OK", command=self._ok).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Скасувати", command=self.destroy).pack(side='right', padx=5)
    
    def _load_data(self):
        for name, item in self.items_map.items():
            if item['id'] == self.line_data.get('item_id'):
                self.item_var.set(name)
                break
        
        self.qty_var.set(str(self.line_data.get('ordered_qty', 0)))
        self.notes_var.set(self.line_data.get('notes', ''))
    
    def _ok(self):
        item = self.items_map.get(self.item_var.get())
        if not item:
            messagebox.showerror("Помилка", "Виберіть товар")
            return
        
        try:
            qty = float(self.qty_var.get() or 0)
        except ValueError:
            messagebox.showerror("Помилка", "Невірна кількість")
            return
        
        if qty <= 0:
            messagebox.showerror("Помилка", "Кількість повинна бути більше 0")
            return
        
        self.result = {
            'item_id': item['id'],
            'sku': item['sku'],
            'item_name': item['name'],
            'ordered_qty': qty,
            'reserved_qty': self.line_data.get('reserved_qty', 0) if self.line_data else 0,
            'picked_qty': self.line_data.get('picked_qty', 0) if self.line_data else 0,
            'shipped_qty': self.line_data.get('shipped_qty', 0) if self.line_data else 0,
            'notes': self.notes_var.get() or None,
        }
        self.destroy()


class InventoryCountListFrame(BaseFrame):
    """Список інвентаризацій"""
    
    def __init__(self, parent, app):
        super().__init__(parent, app)
        self.count_dal = InventoryCountDAL()
        self.count_service = InventoryCountService(
            app.auth_service,
            InventoryService(app.auth_service)
        )
        self._create_widgets()
        self._load_data()
    
    def _create_widgets(self):
        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', padx=5, pady=5)
        
        ttk.Button(toolbar, text="➕ Створити", command=self._create).pack(side='left', padx=2)
        ttk.Button(toolbar, text="✏️ Редагувати", command=self._edit).pack(side='left', padx=2)
        ttk.Button(toolbar, text="📥 Заповнити", command=self._fill).pack(side='left', padx=2)
        ttk.Button(toolbar, text="✅ Провести", command=self._post).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🗑️ Видалити", command=self._delete).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🔄 Оновити", command=self._load_data).pack(side='left', padx=2)
        
        columns = [
            ('id', 'ID', 0),
            ('doc_number', '№ документа', 150),
            ('doc_date', 'Дата', 100),
            ('status', 'Статус', 100),
            ('warehouse_name', 'Склад', 150),
            ('client_name', 'Клієнт', 150),
            ('count_type', 'Тип', 100),
        ]
        self.table = DataTable(self, columns, on_double_click=self._edit_selected)
        self.table.pack(fill='both', expand=True, padx=5, pady=5)
    
    def _load_data(self):
        data = self.count_dal.get_all()
        self.table.load_data(data)
    
    def _create(self):
        if not self.auth.has_permission('inventory_edit'):
            self.show_error("Недостатньо прав")
            return
        InventoryCountEditDialog(self, self.app, None, self._load_data)
    
    def _edit(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        self._edit_selected(selected)
    
    def _edit_selected(self, selected):
        if not self.auth.has_permission('inventory_edit'):
            self.show_error("Недостатньо прав")
            return
        idx = self.table.get_selected_index()
        if idx >= 0:
            count_id = self.table.data[idx]['id']
            InventoryCountEditDialog(self, self.app, count_id, self._load_data)
    
    def _fill(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        
        try:
            idx = self.table.get_selected_index()
            count_id = self.table.data[idx]['id']
            self.count_service.fill_from_balances(count_id)
            self.show_info("Інвентаризацію заповнено")
            self._load_data()
        except Exception as e:
            self.show_error(str(e))
    
    def _post(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        
        if not self.ask_confirm("Провести інвентаризацію?"):
            return
        
        try:
            idx = self.table.get_selected_index()
            count_id = self.table.data[idx]['id']
            self.count_service.post_count(count_id)
            self.show_info("Інвентаризацію проведено")
            self._load_data()
        except Exception as e:
            self.show_error(str(e))
    
    def _delete(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть документ")
            return
        
        if not self.ask_confirm("Видалити документ?"):
            return
        
        try:
            idx = self.table.get_selected_index()
            count_id = self.table.data[idx]['id']
            self.count_dal.delete(count_id)
            self.show_info("Документ видалено")
            self._load_data()
        except Exception as e:
            self.show_error(str(e))


class InventoryCountEditDialog(tk.Toplevel):
    """Діалог редагування інвентаризації"""
    
    def __init__(self, parent, app, count_id: str = None, on_save=None):
        super().__init__(parent)
        self.app = app
        self.count_id = count_id
        self.on_save = on_save
        self.count_dal = InventoryCountDAL()
        self.warehouse_dal = WarehouseDAL()
        self.client_dal = ClientDAL()
        
        self.title("Інвентаризація" if count_id else "Нова інвентаризація")
        self.geometry("800x500")
        self.transient(parent)
        self.grab_set()
        
        self._create_widgets()
        if count_id:
            self._load_data()
    
    def _create_widgets(self):
        # Заголовок
        header_frame = ttk.LabelFrame(self, text="Реквізити", padding=10)
        header_frame.pack(fill='x', padx=10, pady=5)
        
        # Склад
        ttk.Label(header_frame, text="Склад:").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.warehouse_var = tk.StringVar()
        warehouses = self.warehouse_dal.get_all()
        self.warehouses_map = {w['name']: w['id'] for w in warehouses}
        ttk.Combobox(header_frame, textvariable=self.warehouse_var,
                    values=list(self.warehouses_map.keys()), state='readonly', width=30).grid(row=0, column=1, sticky='w', padx=5, pady=5)
        
        # Клієнт
        ttk.Label(header_frame, text="Клієнт:").grid(row=0, column=2, sticky='e', padx=5, pady=5)
        self.client_var = tk.StringVar()
        clients = self.client_dal.get_all()
        self.clients_map = {c['name']: c['id'] for c in clients}
        self.clients_map[''] = None
        ttk.Combobox(header_frame, textvariable=self.client_var,
                    values=[''] + list(self.clients_map.keys())[:-1], state='readonly', width=30).grid(row=0, column=3, sticky='w', padx=5, pady=5)
        
        # Дата
        ttk.Label(header_frame, text="Дата:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.date_var = tk.StringVar(value=today_str())
        ttk.Entry(header_frame, textvariable=self.date_var, width=15).grid(row=1, column=1, sticky='w', padx=5, pady=5)
        
        # Тип
        ttk.Label(header_frame, text="Тип:").grid(row=1, column=2, sticky='e', padx=5, pady=5)
        self.type_var = tk.StringVar(value="full")
        ttk.Combobox(header_frame, textvariable=self.type_var,
                    values=['full', 'partial', 'zone', 'sku'], state='readonly', width=15).grid(row=1, column=3, sticky='w', padx=5, pady=5)
        
        # Позиції
        lines_frame = ttk.LabelFrame(self, text="Позиції", padding=10)
        lines_frame.pack(fill='both', expand=True, padx=10, pady=5)
        
        columns = [
            ('item_name', 'Товар', 200),
            ('location_code', 'Комірка', 100),
            ('system_qty', 'За даними системи', 120),
            ('counted_qty', 'Фактично', 100),
            ('difference', 'Різниця', 100),
        ]
        self.lines_table = DataTable(lines_frame, columns)
        self.lines_table.pack(fill='both', expand=True)
        
        # Кнопки
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(btn_frame, text="💾 Зберегти", command=self._save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="❌ Закрити", command=self.destroy).pack(side='right', padx=5)
    
    def _load_data(self):
        count = self.count_dal.get_by_id(self.count_id)
        if not count:
            return
        
        if count['warehouse_name']:
            self.warehouse_var.set(count['warehouse_name'])
        if count['client_name']:
            self.client_var.set(count['client_name'])
        
        self.date_var.set(count['doc_date'])
        self.type_var.set(count['count_type'] or 'full')
        
        lines = self.count_dal.get_lines(self.count_id)
        self.lines_table.load_data(lines)
    
    def _save(self):
        warehouse_id = self.warehouses_map.get(self.warehouse_var.get())
        if not warehouse_id:
            messagebox.showerror("Помилка", "Виберіть склад")
            return
        
        try:
            data = {
                'warehouse_id': warehouse_id,
                'client_id': self.clients_map.get(self.client_var.get()),
                'doc_date': self.date_var.get(),
                'count_type': self.type_var.get(),
            }
            
            if not self.count_id:
                user_id = self.app.auth_service.current_user['id'] if self.app.auth_service.current_user else None
                self.count_id = self.count_dal.create(data, user_id)
            
            messagebox.showinfo("Інформація", "Збережено")
            if self.on_save:
                self.on_save()
            self.destroy()
        except Exception as e:
            messagebox.showerror("Помилка", str(e))


# ============================================================================
# ЗВІТИ
# ============================================================================

class ReportsFrame(BaseFrame):
    """Фрейм звітів"""
    
    def __init__(self, parent, app):
        super().__init__(parent, app)
        self.report_service = ReportService()
        self._create_widgets()
    
    def _create_widgets(self):
        # Ліве меню звітів
        left_frame = ttk.Frame(self, width=200)
        left_frame.pack(side='left', fill='y', padx=5, pady=5)
        left_frame.pack_propagate(False)
        
        ttk.Label(left_frame, text="Оберіть звіт:", font=('Arial', 10, 'bold')).pack(pady=10)
        
        reports = [
            ("Залишки на складі", self._show_balance_report),
            ("Рух товарів", self._show_moves_report),
            ("Оборотність", self._show_turnover_report),
            ("Терміни придатності", self._show_expiry_report),
            ("Замовлення (вхідні)", self._show_inbound_report),
            ("Замовлення (вихідні)", self._show_outbound_report),
            ("Продуктивність", self._show_productivity_report),
        ]
        
        for name, command in reports:
            ttk.Button(left_frame, text=name, command=command, width=25).pack(pady=2)
        
        # Права частина - результати
        self.right_frame = ttk.Frame(self)
        self.right_frame.pack(side='right', fill='both', expand=True, padx=5, pady=5)
        
        # Фільтри
        self.filter_frame = ttk.LabelFrame(self.right_frame, text="Фільтри", padding=10)
        self.filter_frame.pack(fill='x', pady=5)
        
        # Таблиця результатів
        self.result_frame = ttk.LabelFrame(self.right_frame, text="Результати", padding=10)
        self.result_frame.pack(fill='both', expand=True, pady=5)
        
        self.result_table = None
        self.current_data = []
    
    def _clear_filters(self):
        for widget in self.filter_frame.winfo_children():
            widget.destroy()
    
    def _clear_results(self):
        for widget in self.result_frame.winfo_children():
            widget.destroy()
    
    def _create_export_button(self):
        ttk.Button(self.filter_frame, text="📥 Експорт CSV", command=self._export_csv).pack(side='right', padx=5)
    
    def _export_csv(self):
        if not self.current_data:
            self.show_warning("Немає даних для експорту")
            return
        
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")]
        )
        if filename:
            try:
                self.report_service.export_to_csv(self.current_data, filename)
                self.show_info(f"Дані експортовано в {filename}")
            except Exception as e:
                self.show_error(str(e))
    
    def _show_balance_report(self):
        self._clear_filters()
        self._clear_results()
        
        # Фільтри
        ttk.Label(self.filter_frame, text="Клієнт:").pack(side='left', padx=5)
        client_var = tk.StringVar()
        clients = ClientDAL().get_all()
        clients_map = {c['name']: c['id'] for c in clients}
        clients_map['Всі'] = None
        ttk.Combobox(self.filter_frame, textvariable=client_var,
                    values=['Всі'] + [c['name'] for c in clients], state='readonly', width=20).pack(side='left', padx=5)
        
        def load_report():
            client_id = clients_map.get(client_var.get())
            self.current_data = self.report_service.get_stock_balance_report(client_id=client_id)
            
            columns = [
                ('sku', 'Артикул', 100),
                ('item_name', 'Товар', 200),
                ('client_name', 'Клієнт', 150),
                ('warehouse_name', 'Склад', 120),
                ('location_code', 'Комірка', 100),
                ('qty_available', 'Доступно', 80),
                ('qty_reserved', 'Резерв', 80),
                ('qty_blocked', 'Заблоковано', 80),
            ]
            
            self._clear_results()
            self.result_table = DataTable(self.result_frame, columns)
            self.result_table.pack(fill='both', expand=True)
            self.result_table.load_data(self.current_data)
        
        ttk.Button(self.filter_frame, text="Сформувати", command=load_report).pack(side='left', padx=10)
        self._create_export_button()
        
        load_report()
    
    def _show_moves_report(self):
        self._clear_filters()
        self._clear_results()
        
        ttk.Label(self.filter_frame, text="Дата з:").pack(side='left', padx=5)
        date_from_var = tk.StringVar(value=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        ttk.Entry(self.filter_frame, textvariable=date_from_var, width=12).pack(side='left', padx=5)
        
        ttk.Label(self.filter_frame, text="Дата по:").pack(side='left', padx=5)
        date_to_var = tk.StringVar(value=today_str())
        ttk.Entry(self.filter_frame, textvariable=date_to_var, width=12).pack(side='left', padx=5)
        
        def load_report():
            self.current_data = self.report_service.get_stock_moves_report(
                date_from=date_from_var.get(),
                date_to=date_to_var.get()
            )
            
            columns = [
                ('created_at', 'Дата', 150),
                ('move_type', 'Тип', 100),
                ('sku', 'Артикул', 100),
                ('item_name', 'Товар', 150),
                ('qty', 'Кількість', 80),
                ('client_name', 'Клієнт', 120),
                ('username', 'Користувач', 100),
            ]
            
            self._clear_results()
            self.result_table = DataTable(self.result_frame, columns)
            self.result_table.pack(fill='both', expand=True)
            self.result_table.load_data(self.current_data)
        
        ttk.Button(self.filter_frame, text="Сформувати", command=load_report).pack(side='left', padx=10)
        self._create_export_button()
        
        load_report()
    
    def _show_turnover_report(self):
        self._clear_filters()
        self._clear_results()
        
        ttk.Label(self.filter_frame, text="Дата з:").pack(side='left', padx=5)
        date_from_var = tk.StringVar(value=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        ttk.Entry(self.filter_frame, textvariable=date_from_var, width=12).pack(side='left', padx=5)
        
        ttk.Label(self.filter_frame, text="Дата по:").pack(side='left', padx=5)
        date_to_var = tk.StringVar(value=today_str())
        ttk.Entry(self.filter_frame, textvariable=date_to_var, width=12).pack(side='left', padx=5)
        
        def load_report():
            self.current_data = self.report_service.get_turnover_report(
                date_from=date_from_var.get(),
                date_to=date_to_var.get()
            )
            
            columns = [
                ('sku', 'Артикул', 100),
                ('item_name', 'Товар', 200),
                ('client_name', 'Клієнт', 150),
                ('qty_in', 'Прихід', 100),
                ('qty_out', 'Розхід', 100),
                ('move_count', 'К-сть операцій', 100),
            ]
            
            self._clear_results()
            self.result_table = DataTable(self.result_frame, columns)
            self.result_table.pack(fill='both', expand=True)
            self.result_table.load_data(self.current_data)
        
        ttk.Button(self.filter_frame, text="Сформувати", command=load_report).pack(side='left', padx=10)
        self._create_export_button()
        
        load_report()
    
    def _show_expiry_report(self):
        self._clear_filters()
        self._clear_results()
        
        ttk.Label(self.filter_frame, text="Днів вперед:").pack(side='left', padx=5)
        days_var = tk.StringVar(value="30")
        ttk.Entry(self.filter_frame, textvariable=days_var, width=10).pack(side='left', padx=5)
        
        def load_report():
            try:
                days = int(days_var.get() or 30)
            except ValueError:
                days = 30
            
            self.current_data = self.report_service.get_expiry_report(days_ahead=days)
            
            columns = [
                ('sku', 'Артикул', 100),
                ('item_name', 'Товар', 200),
                ('client_name', 'Клієнт', 150),
                ('expiry_date', 'Термін придатності', 120),
                ('expiry_status', 'Статус', 120),
                ('qty_available', 'Кількість', 100),
            ]
            
            self._clear_results()
            self.result_table = DataTable(self.result_frame, columns)
            self.result_table.pack(fill='both', expand=True)
            self.result_table.load_data(self.current_data)
        
        ttk.Button(self.filter_frame, text="Сформувати", command=load_report).pack(side='left', padx=10)
        self._create_export_button()
        
        load_report()
    
    def _show_inbound_report(self):
        self._clear_filters()
        self._clear_results()
        
        ttk.Label(self.filter_frame, text="Дата з:").pack(side='left', padx=5)
        date_from_var = tk.StringVar(value=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        ttk.Entry(self.filter_frame, textvariable=date_from_var, width=12).pack(side='left', padx=5)
        
        ttk.Label(self.filter_frame, text="Дата по:").pack(side='left', padx=5)
        date_to_var = tk.StringVar(value=today_str())
        ttk.Entry(self.filter_frame, textvariable=date_to_var, width=12).pack(side='left', padx=5)
        
        ttk.Label(self.filter_frame, text="Статус:").pack(side='left', padx=5)
        status_var = tk.StringVar()
        statuses = ['', DocStatus.DRAFT.value, DocStatus.IN_PROGRESS.value,
                   DocStatus.POSTED.value, DocStatus.CANCELLED.value]
        ttk.Combobox(self.filter_frame, textvariable=status_var, values=statuses,
                    state='readonly', width=15).pack(side='left', padx=5)
        
        def load_report():
            self.current_data = self.report_service.get_orders_report(
                date_from=date_from_var.get() or None,
                date_to=date_to_var.get() or None,
                status=status_var.get() or None,
                order_type='inbound'
            )
            
            columns = [
                ('doc_number', '№ документа', 150),
                ('doc_date', 'Дата', 100),
                ('status', 'Статус', 100),
                ('client_name', 'Клієнт', 150),
                ('supplier_name', 'Постачальник', 150),
                ('line_count', 'Позицій', 70),
                ('total_expected', 'Очікувано', 100),
                ('total_received', 'Прийнято', 100),
            ]
            
            self._clear_results()
            self.result_table = DataTable(self.result_frame, columns)
            self.result_table.pack(fill='both', expand=True)
            self.result_table.load_data(self.current_data)
        
        ttk.Button(self.filter_frame, text="Сформувати", command=load_report).pack(side='left', padx=10)
        self._create_export_button()
        
        load_report()
    
    def _show_outbound_report(self):
        self._clear_filters()
        self._clear_results()
        
        ttk.Label(self.filter_frame, text="Дата з:").pack(side='left', padx=5)
        date_from_var = tk.StringVar(value=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
        ttk.Entry(self.filter_frame, textvariable=date_from_var, width=12).pack(side='left', padx=5)
        
        ttk.Label(self.filter_frame, text="Дата по:").pack(side='left', padx=5)
        date_to_var = tk.StringVar(value=today_str())
        ttk.Entry(self.filter_frame, textvariable=date_to_var, width=12).pack(side='left', padx=5)
        
        ttk.Label(self.filter_frame, text="Статус:").pack(side='left', padx=5)
        status_var = tk.StringVar()
        statuses = ['', DocStatus.DRAFT.value, DocStatus.IN_PROGRESS.value,
                   DocStatus.POSTED.value, DocStatus.CANCELLED.value]
        ttk.Combobox(self.filter_frame, textvariable=status_var, values=statuses,
                    state='readonly', width=15).pack(side='left', padx=5)
        
        def load_report():
            self.current_data = self.report_service.get_orders_report(
                date_from=date_from_var.get() or None,
                date_to=date_to_var.get() or None,
                status=status_var.get() or None,
                order_type='outbound'
            )
            
            columns = [
                ('doc_number', '№ документа', 150),
                ('doc_date', 'Дата', 100),
                ('status', 'Статус', 100),
                ('client_name', 'Клієнт', 150),
                ('carrier_name', 'Перевізник', 120),
                ('line_count', 'Позицій', 70),
                ('total_ordered', 'Замовлено', 100),
                ('total_shipped', 'Відвантажено', 100),
            ]
            
            self._clear_results()
            self.result_table = DataTable(self.result_frame, columns)
            self.result_table.pack(fill='both', expand=True)
            self.result_table.load_data(self.current_data)
        
        ttk.Button(self.filter_frame, text="Сформувати", command=load_report).pack(side='left', padx=10)
        self._create_export_button()
        
        load_report()
    
    def _show_productivity_report(self):
        self._clear_filters()
        self._clear_results()
        
        ttk.Label(self.filter_frame, text="Дата з:").pack(side='left', padx=5)
        date_from_var = tk.StringVar(value=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
        ttk.Entry(self.filter_frame, textvariable=date_from_var, width=12).pack(side='left', padx=5)
        
        ttk.Label(self.filter_frame, text="Дата по:").pack(side='left', padx=5)
        date_to_var = tk.StringVar(value=today_str())
        ttk.Entry(self.filter_frame, textvariable=date_to_var, width=12).pack(side='left', padx=5)
        
        def load_report():
            self.current_data = self.report_service.get_worker_productivity_report(
                date_from=date_from_var.get(),
                date_to=date_to_var.get()
            )
            
            columns = [
                ('username', 'Користувач', 120),
                ('full_name', 'ПІБ', 200),
                ('move_type', 'Тип операції', 150),
                ('operation_count', 'К-сть операцій', 120),
                ('total_qty', 'Всього одиниць', 120),
            ]
            
            self._clear_results()
            self.result_table = DataTable(self.result_frame, columns)
            self.result_table.pack(fill='both', expand=True)
            self.result_table.load_data(self.current_data)
        
        ttk.Button(self.filter_frame, text="Сформувати", command=load_report).pack(side='left', padx=10)
        self._create_export_button()
        
        load_report()


# ============================================================================
# АДМІНІСТРУВАННЯ
# ============================================================================

class UserListFrame(BaseFrame):
    """Список користувачів"""
    
    def __init__(self, parent, app):
        super().__init__(parent, app)
        self.user_dal = UserDAL()
        self._create_widgets()
        self._load_data()
    
    def _create_widgets(self):
        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', padx=5, pady=5)
        
        ttk.Button(toolbar, text="➕ Створити", command=self._create).pack(side='left', padx=2)
        ttk.Button(toolbar, text="✏️ Редагувати", command=self._edit).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🗑️ Видалити", command=self._delete).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🔄 Оновити", command=self._load_data).pack(side='left', padx=2)
        
        columns = [
            ('id', 'ID', 0),
            ('username', 'Логін', 120),
            ('full_name', 'ПІБ', 200),
            ('email', 'Email', 180),
            ('role_name', 'Роль', 150),
            ('is_active', 'Активний', 80),
            ('last_login', 'Останній вхід', 150),
        ]
        self.table = DataTable(self, columns, on_double_click=self._edit_selected)
        self.table.pack(fill='both', expand=True, padx=5, pady=5)
    
    def _load_data(self):
        data = self.user_dal.get_all()
        for row in data:
            row['is_active'] = 'Так' if row['is_active'] else 'Ні'
        self.table.load_data(data)
    
    def _create(self):
        if not self.auth.has_permission('users_edit'):
            self.show_error("Недостатньо прав")
            return
        UserEditDialog(self, self.app, None, self._load_data)
    
    def _edit(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть користувача")
            return
        self._edit_selected(selected)
    
    def _edit_selected(self, selected):
        if not self.auth.has_permission('users_edit'):
            self.show_error("Недостатньо прав")
            return
        idx = self.table.get_selected_index()
        if idx >= 0:
            user_id = self.table.data[idx]['id']
            UserEditDialog(self, self.app, user_id, self._load_data)
    
    def _delete(self):
        if not self.auth.has_permission('users_delete'):
            self.show_error("Недостатньо прав")
            return
        
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть користувача")
            return
        
        if selected['username'] == 'admin':
            self.show_error("Неможливо видалити адміністратора")
            return
        
        if not self.ask_confirm(f"Видалити користувача {selected['username']}?"):
            return
        
        try:
            idx = self.table.get_selected_index()
            user_id = self.table.data[idx]['id']
            self.user_dal.delete(user_id)
            self.show_info("Користувача видалено")
            self._load_data()
        except Exception as e:
            self.show_error(str(e))


class UserEditDialog(tk.Toplevel):
    """Діалог редагування користувача"""
    
    def __init__(self, parent, app, user_id: str = None, on_save=None):
        super().__init__(parent)
        self.app = app
        self.user_id = user_id
        self.on_save = on_save
        self.user_dal = UserDAL()
        self.role_dal = RoleDAL()
        self.client_dal = ClientDAL()
        
        self.title("Картка користувача" if user_id else "Новий користувач")
        self.geometry("450x400")
        self.transient(parent)
        self.grab_set()
        
        self._create_widgets()
        if user_id:
            self._load_data()
    
    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(fill='both', expand=True)
        
        # Логін
        ttk.Label(main_frame, text="Логін:").grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.username_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.username_var, width=30).grid(row=0, column=1, sticky='w', padx=5, pady=5)
        
        # Пароль
        ttk.Label(main_frame, text="Пароль:").grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.password_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.password_var, show='*', width=30).grid(row=1, column=1, sticky='w', padx=5, pady=5)
        if self.user_id:
            ttk.Label(main_frame, text="(залиште порожнім, щоб не змінювати)").grid(row=1, column=2, sticky='w')
        
        # ПІБ
        ttk.Label(main_frame, text="ПІБ:").grid(row=2, column=0, sticky='e', padx=5, pady=5)
        self.full_name_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.full_name_var, width=40).grid(row=2, column=1, sticky='w', padx=5, pady=5)
        
        # Email
        ttk.Label(main_frame, text="Email:").grid(row=3, column=0, sticky='e', padx=5, pady=5)
        self.email_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.email_var, width=30).grid(row=3, column=1, sticky='w', padx=5, pady=5)
        
        # Роль
        ttk.Label(main_frame, text="Роль:").grid(row=4, column=0, sticky='e', padx=5, pady=5)
        self.role_var = tk.StringVar()
        roles = self.role_dal.get_all()
        self.roles_map = {r['name']: r['id'] for r in roles}
        ttk.Combobox(main_frame, textvariable=self.role_var,
                    values=list(self.roles_map.keys()), state='readonly', width=27).grid(row=4, column=1, sticky='w', padx=5, pady=5)
        
        # Клієнт (для 3PL)
        ttk.Label(main_frame, text="Клієнт 3PL:").grid(row=5, column=0, sticky='e', padx=5, pady=5)
        self.client_var = tk.StringVar()
        clients = self.client_dal.get_all()
        self.clients_map = {c['name']: c['id'] for c in clients}
        self.clients_map[''] = None
        ttk.Combobox(main_frame, textvariable=self.client_var,
                    values=[''] + [c['name'] for c in clients], state='readonly', width=27).grid(row=5, column=1, sticky='w', padx=5, pady=5)
        
        # Активний
        self.is_active_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(main_frame, text="Активний", variable=self.is_active_var).grid(row=6, column=1, sticky='w', padx=5, pady=5)
        
        # Кнопки
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(btn_frame, text="Зберегти", command=self._save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Закрити", command=self.destroy).pack(side='right', padx=5)
    
    def _load_data(self):
        user = self.user_dal.get_by_id(self.user_id)
        if not user:
            return
        
        self.username_var.set(user['username'])
        self.full_name_var.set(user['full_name'] or '')
        self.email_var.set(user['email'] or '')
        
        if user['role_name']:
            self.role_var.set(user['role_name'])
        
        # Клієнт
        if user['client_id']:
            for name, cid in self.clients_map.items():
                if cid == user['client_id']:
                    self.client_var.set(name)
                    break
        
        self.is_active_var.set(bool(user['is_active']))
    
    def _save(self):
        username = self.username_var.get().strip()
        
        if not username:
            messagebox.showerror("Помилка", "Введіть логін")
            return
        
        if not self.user_id and not self.password_var.get():
            messagebox.showerror("Помилка", "Введіть пароль")
            return
        
        try:
            data = {
                'username': username,
                'full_name': self.full_name_var.get().strip() or None,
                'email': self.email_var.get().strip() or None,
                'role_id': self.roles_map.get(self.role_var.get()),
                'client_id': self.clients_map.get(self.client_var.get()),
                'is_active': 1 if self.is_active_var.get() else 0,
            }
            
            if self.password_var.get():
                data['password'] = self.password_var.get()
            
            if self.user_id:
                self.user_dal.update(self.user_id, data)
            else:
                data['password'] = self.password_var.get()
                self.user_dal.create(data)
            
            messagebox.showinfo("Інформація", "Збережено")
            if self.on_save:
                self.on_save()
            self.destroy()
        except Exception as e:
            messagebox.showerror("Помилка", str(e))


class RoleListFrame(BaseFrame):
    """Список ролей"""
    
    def __init__(self, parent, app):
        super().__init__(parent, app)
        self.role_dal = RoleDAL()
        self._create_widgets()
        self._load_data()
    
    def _create_widgets(self):
        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', padx=5, pady=5)
        
        ttk.Button(toolbar, text="✏️ Права доступу", command=self._edit_permissions).pack(side='left', padx=2)
        ttk.Button(toolbar, text="🔄 Оновити", command=self._load_data).pack(side='left', padx=2)
        
        columns = [
            ('id', 'ID', 0),
            ('name', 'Назва', 200),
            ('description', 'Опис', 300),
            ('is_system', 'Системна', 80),
        ]
        self.table = DataTable(self, columns, on_double_click=self._edit_permissions_selected)
        self.table.pack(fill='both', expand=True, padx=5, pady=5)
    
    def _load_data(self):
        data = self.role_dal.get_all()
        for row in data:
            row['is_system'] = 'Так' if row['is_system'] else 'Ні'
        self.table.load_data(data)
    
    def _edit_permissions(self):
        selected = self.table.get_selected()
        if not selected:
            self.show_warning("Виберіть роль")
            return
        self._edit_permissions_selected(selected)
    
    def _edit_permissions_selected(self, selected):
        if not self.auth.has_permission('roles_edit'):
            self.show_error("Недостатньо прав")
            return
        idx = self.table.get_selected_index()
        if idx >= 0:
            role_id = self.table.data[idx]['id']
            RolePermissionsDialog(self, self.app, role_id)


class RolePermissionsDialog(tk.Toplevel):
    """Діалог редагування прав ролі"""
    
    def __init__(self, parent, app, role_id: str):
        super().__init__(parent)
        self.app = app
        self.role_id = role_id
        self.role_dal = RoleDAL()
        
        role = self.role_dal.get_by_id(role_id)
        self.title(f"Права ролі: {role['name']}" if role else "Права ролі")
        self.geometry("500x600")
        self.transient(parent)
        self.grab_set()
        
        self._create_widgets()
        self._load_data()
    
    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=10)
        main_frame.pack(fill='both', expand=True)
        
        ttk.Label(main_frame, text="Оберіть права доступу:", font=('Arial', 10, 'bold')).pack(pady=10)
        
        # Фрейм зі скролом для чекбоксів
        canvas = tk.Canvas(main_frame)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Чекбокси для прав
        self.perm_vars = {}
        perm_labels = {
            'users_view': 'Користувачі: перегляд',
            'users_edit': 'Користувачі: редагування',
            'users_delete': 'Користувачі: видалення',
            'roles_view': 'Ролі: перегляд',
            'roles_edit': 'Ролі: редагування',
            'items_view': 'Номенклатура: перегляд',
            'items_edit': 'Номенклатура: редагування',
            'items_delete': 'Номенклатура: видалення',
            'items_import': 'Номенклатура: імпорт',
            'clients_view': 'Клієнти: перегляд',
            'clients_edit': 'Клієнти: редагування',
            'clients_delete': 'Клієнти: видалення',
            'suppliers_view': 'Постачальники: перегляд',
            'suppliers_edit': 'Постачальники: редагування',
            'suppliers_delete': 'Постачальники: видалення',
            'warehouses_view': 'Склади: перегляд',
            'warehouses_edit': 'Склади: редагування',
            'zones_view': 'Зони: перегляд',
            'zones_edit': 'Зони: редагування',
            'locations_view': 'Комірки: перегляд',
            'locations_edit': 'Комірки: редагування',
            'inbound_view': 'Приймання: перегляд',
            'inbound_edit': 'Приймання: редагування',
            'inbound_post': 'Приймання: проведення',
            'outbound_view': 'Відвантаження: перегляд',
            'outbound_edit': 'Відвантаження: редагування',
            'outbound_post': 'Відвантаження: проведення',
            'inventory_view': 'Інвентаризація: перегляд',
            'inventory_edit': 'Інвентаризація: редагування',
            'inventory_post': 'Інвентаризація: проведення',
            'returns_view': 'Повернення: перегляд',
            'returns_edit': 'Повернення: редагування',
            'returns_post': 'Повернення: проведення',
            'writeoffs_view': 'Списання: перегляд',
            'writeoffs_edit': 'Списання: редагування',
            'writeoffs_post': 'Списання: проведення',
            'reports_view': 'Звіти: перегляд',
            'reports_export': 'Звіти: експорт',
            'audit_view': 'Аудит: перегляд',
            'settings_view': 'Налаштування: перегляд',
            'settings_edit': 'Налаштування: редагування',
        }
        
        for perm in PERMISSIONS:
            var = tk.BooleanVar()
            label = perm_labels.get(perm, perm)
            ttk.Checkbutton(scrollable_frame, text=label, variable=var).pack(anchor='w', pady=2)
            self.perm_vars[perm] = var
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Кнопки
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)
        
        ttk.Button(btn_frame, text="Зберегти", command=self._save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Закрити", command=self.destroy).pack(side='right', padx=5)
    
    def _load_data(self):
        perms = self.role_dal.get_permissions(self.role_id)
        for perm in perms:
            if perm in self.perm_vars:
                self.perm_vars[perm].set(True)
    
    def _save(self):
        try:
            selected_perms = [perm for perm, var in self.perm_vars.items() if var.get()]
            self.role_dal.set_permissions(self.role_id, selected_perms)
            messagebox.showinfo("Інформація", "Права збережено")
            self.destroy()
        except Exception as e:
            messagebox.showerror("Помилка", str(e))


class AuditLogFrame(BaseFrame):
    """Журнал аудиту"""
    
    def __init__(self, parent, app):
        super().__init__(parent, app)
        self.audit_dal = AuditDAL()
        self._create_widgets()
        self._load_data()
    
    def _create_widgets(self):
        # Фільтри
        filter_frame = ttk.Frame(self)
        filter_frame.pack(fill='x', padx=5, pady=5)
        
        ttk.Label(filter_frame, text="Дата з:").pack(side='left', padx=5)
        self.date_from_var = tk.StringVar(value=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
        ttk.Entry(filter_frame, textvariable=self.date_from_var, width=12).pack(side='left', padx=5)
        
        ttk.Label(filter_frame, text="Дата по:").pack(side='left', padx=5)
        self.date_to_var = tk.StringVar(value=today_str())
        ttk.Entry(filter_frame, textvariable=self.date_to_var, width=12).pack(side='left', padx=5)
        
        ttk.Button(filter_frame, text="🔄 Оновити", command=self._load_data).pack(side='left', padx=10)
        
        columns = [
            ('created_at', 'Дата/час', 150),
            ('username', 'Користувач', 120),
            ('action', 'Дія', 150),
            ('entity_type', 'Тип об\'єкта', 120),
            ('entity_id', 'ID об\'єкта', 150),
        ]
        self.table = DataTable(self, columns)
        self.table.pack(fill='both', expand=True, padx=5, pady=5)
    
    def _load_data(self):
        data = self.audit_dal.get_logs(
            date_from=self.date_from_var.get() or None,
            date_to=self.date_to_var.get() or None
        )
        self.table.load_data(data)


# ============================================================================
# ГОЛОВНЕ ВІКНО
# ============================================================================

class LoginDialog(tk.Toplevel):
    """Діалог входу в систему"""
    
    def __init__(self, parent, auth_service: AuthService):
        super().__init__(parent)
        self.auth_service = auth_service
        self.result = False
        
        self.title("Вхід в систему")
        self.geometry("350x200")
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()
        
        # Центрування вікна
        self.update_idletasks()
        x = (self.winfo_screenwidth() - self.winfo_width()) // 2
        y = (self.winfo_screenheight() - self.winfo_height()) // 2
        self.geometry(f"+{x}+{y}")
        
        self._create_widgets()
        
        self.protocol("WM_DELETE_WINDOW", self._on_close)
    
    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=20)
        main_frame.pack(fill='both', expand=True)
        
        ttk.Label(main_frame, text="WMS - Складська система", 
                 font=('Arial', 14, 'bold')).pack(pady=10)
        
        # Логін
        login_frame = ttk.Frame(main_frame)
        login_frame.pack(fill='x', pady=5)
        ttk.Label(login_frame, text="Логін:", width=10).pack(side='left')
        self.username_var = tk.StringVar()
        self.username_entry = ttk.Entry(login_frame, textvariable=self.username_var, width=25)
        self.username_entry.pack(side='left', padx=5)
        self.username_entry.focus()
        
        # Пароль
        pwd_frame = ttk.Frame(main_frame)
        pwd_frame.pack(fill='x', pady=5)
        ttk.Label(pwd_frame, text="Пароль:", width=10).pack(side='left')
        self.password_var = tk.StringVar()
        self.password_entry = ttk.Entry(pwd_frame, textvariable=self.password_var, show='*', width=25)
        self.password_entry.pack(side='left', padx=5)
        
        # Кнопки
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(pady=15)
        ttk.Button(btn_frame, text="Увійти", command=self._login, width=15).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Вихід", command=self._on_close, width=15).pack(side='left', padx=5)
        
        # Bind Enter key
        self.bind('<Return>', lambda e: self._login())
    
    def _login(self):
        username = self.username_var.get().strip()
        password = self.password_var.get()
        
        if not username or not password:
            messagebox.showwarning("Увага", "Введіть логін та пароль")
            return
        
        success, message = self.auth_service.login(username, password)
        
        if success:
            self.result = True
            self.destroy()
        else:
            messagebox.showerror("Помилка входу", message)
            self.password_var.set('')
            self.password_entry.focus()
    
    def _on_close(self):
        self.result = False
        self.destroy()


class MainApplication(tk.Tk):
    """Головне вікно додатку"""
    
    def __init__(self):
        super().__init__()
        
        self.title(f"{APP_NAME} v{APP_VERSION}")
        self.geometry("1400x800")
        self.state('zoomed')  # Максимізація вікна
        
        # Ініціалізація сервісів
        self.db = DatabaseManager()
        self.auth_service = AuthService()
        
        # Налаштування стилів
        self._setup_styles()
        
        # Показуємо діалог входу
        self.withdraw()  # Приховуємо головне вікно
        
        login_dialog = LoginDialog(self, self.auth_service)
        self.wait_window(login_dialog)
        
        if not login_dialog.result:
            self.destroy()
            return
        
        self.deiconify()  # Показуємо головне вікно
        
        # Створюємо інтерфейс
        self._create_widgets()
        
        # Показуємо стартову сторінку
        self._show_home()
        
        # Обробка закриття
        self.protocol("WM_DELETE_WINDOW", self._on_close)
    
    def _setup_styles(self):
        """Налаштування стилів"""
        style = ttk.Style()
        style.theme_use('clam')
        
        # Основні кольори
        style.configure('TFrame', background=COLORS['bg'])
        style.configure('TLabel', background=COLORS['bg'], foreground=COLORS['text'])
        style.configure('TButton', padding=5)
        
        # Стиль для меню
        style.configure('Menu.TFrame', background=COLORS['menu_bg'])
        style.configure('Menu.TButton', padding=10, width=20)
        
        # Стиль для панелі інструментів
        style.configure('Toolbar.TFrame', background=COLORS['toolbar_bg'])
        style.configure('Toolbar.TButton', padding=5)
        
        # Стиль для Treeview
        style.configure('Treeview', rowheight=25)
        style.configure('Treeview.Heading', font=('Arial', 9, 'bold'))
    
    def _create_widgets(self):
        """Створення основних віджетів"""
        # Верхня панель
        self._create_toolbar()
        
        # Основний контейнер
        main_container = ttk.Frame(self)
        main_container.pack(fill='both', expand=True)
        
        # Ліве меню
        self._create_menu(main_container)
        
        # Робоча область
        self.work_area = ttk.Frame(main_container)
        self.work_area.pack(side='right', fill='both', expand=True)
        
        # Статус бар
        self._create_statusbar()
    
    def _create_toolbar(self):
        """Створення панелі інструментів"""
        toolbar = ttk.Frame(self, style='Toolbar.TFrame')
        toolbar.pack(fill='x', padx=5, pady=5)
        
        # Ліва частина - кнопки
        left_frame = ttk.Frame(toolbar)
        left_frame.pack(side='left')
        
        ttk.Button(left_frame, text="🏠 Головна", command=self._show_home).pack(side='left', padx=2)
        ttk.Button(left_frame, text="🔄 Оновити", command=self._refresh).pack(side='left', padx=2)
        
        # Права частина - інформація про користувача
        right_frame = ttk.Frame(toolbar)
        right_frame.pack(side='right')
        
        user = self.auth_service.current_user
        user_text = f"👤 {user['full_name'] or user['username']} ({user['role_name']})"
        ttk.Label(right_frame, text=user_text).pack(side='left', padx=10)
        ttk.Button(right_frame, text="🚪 Вихід", command=self._logout).pack(side='left', padx=2)
    
    def _create_menu(self, parent):
        """Створення бокового меню"""
        menu_frame = ttk.Frame(parent, style='Menu.TFrame', width=220)
        menu_frame.pack(side='left', fill='y')
        menu_frame.pack_propagate(False)
        
        # Заголовок меню
        ttk.Label(menu_frame, text="📋 МЕНЮ", font=('Arial', 12, 'bold')).pack(pady=10)
        
        ttk.Separator(menu_frame, orient='horizontal').pack(fill='x', padx=10, pady=5)
        
        # Пункти меню
        menu_items = [
            ("📁 Довідники", [
                ("📦 Номенклатура", self._show_items),
                ("👥 Клієнти 3PL", self._show_clients),
                ("🏭 Постачальники", self._show_suppliers),
                ("🏢 Склади", self._show_warehouses),
                ("📍 Зони", self._show_zones),
                ("📍 Комірки", self._show_locations),
            ]),
            ("📄 Документи", [
                ("📥 Приймання", self._show_inbound),
                ("📤 Відвантаження", self._show_outbound),
                ("📊 Інвентаризація", self._show_inventory_count),
            ]),
            ("📊 Звіти", [
                ("📈 Всі звіти", self._show_reports),
            ]),
            ("⚙️ Адміністрування", [
                ("👤 Користувачі", self._show_users),
                ("🔐 Ролі", self._show_roles),
                ("📝 Журнал аудиту", self._show_audit),
            ]),
        ]
        
        for section_name, items in menu_items:
            # Секція
            section_frame = ttk.LabelFrame(menu_frame, text=section_name, padding=5)
            section_frame.pack(fill='x', padx=5, pady=5)
            
            for item_name, command in items:
                btn = ttk.Button(section_frame, text=item_name, command=command, width=22)
                btn.pack(pady=2)
    
    def _create_statusbar(self):
        """Створення статус бару"""
        statusbar = ttk.Frame(self)
        statusbar.pack(fill='x', side='bottom')
        
        ttk.Separator(statusbar, orient='horizontal').pack(fill='x')
        
        status_frame = ttk.Frame(statusbar)
        status_frame.pack(fill='x', padx=10, pady=5)
        
        self.status_label = ttk.Label(status_frame, text="Готово")
        self.status_label.pack(side='left')
        
        ttk.Label(status_frame, text=f"База даних: {DB_FILE}").pack(side='right')
    
    def _clear_work_area(self):
        """Очищення робочої області"""
        for widget in self.work_area.winfo_children():
            widget.destroy()
    
    def _show_home(self):
        """Показ головної сторінки"""
        self._clear_work_area()
        
        home_frame = ttk.Frame(self.work_area)
        home_frame.pack(fill='both', expand=True, padx=20, pady=20)
        
        ttk.Label(home_frame, text="🏭 WMS - Складська Система Управління",
                 font=('Arial', 24, 'bold')).pack(pady=20)
        
        ttk.Label(home_frame, text="Ласкаво просимо!",
                 font=('Arial', 14)).pack(pady=10)
        
        # Статистика
        stats_frame = ttk.LabelFrame(home_frame, text="Статистика", padding=20)
        stats_frame.pack(fill='x', pady=20)
        
        # Отримуємо статистику
        item_count = len(ItemDAL().get_all(active_only=False))
        client_count = len(ClientDAL().get_all(active_only=False))
        warehouse_count = len(WarehouseDAL().get_all(active_only=False))
        
        inbound_dal = InboundOrderDAL()
        outbound_dal = OutboundOrderDAL()
        
        inbound_draft = len(inbound_dal.get_all(status=DocStatus.DRAFT.value))
        outbound_draft = len(outbound_dal.get_all(status=DocStatus.DRAFT.value))
        
        stats = [
            (f"📦 Товарів: {item_count}", f"👥 Клієнтів: {client_count}"),
            (f"🏢 Складів: {warehouse_count}", f"📥 Приймань (чернетки): {inbound_draft}"),
            (f"📤 Відвантажень (чернетки): {outbound_draft}", ""),
        ]
        
        for row_idx, (col1, col2) in enumerate(stats):
            ttk.Label(stats_frame, text=col1, font=('Arial', 11)).grid(row=row_idx, column=0, sticky='w', padx=20, pady=5)
            if col2:
                ttk.Label(stats_frame, text=col2, font=('Arial', 11)).grid(row=row_idx, column=1, sticky='w', padx=20, pady=5)
        
        # Швидкі дії
        actions_frame = ttk.LabelFrame(home_frame, text="Швидкі дії", padding=20)
        actions_frame.pack(fill='x', pady=20)
        
        ttk.Button(actions_frame, text="📥 Нове приймання", 
                  command=lambda: self._quick_create_inbound()).pack(side='left', padx=10)
        ttk.Button(actions_frame, text="📤 Нове відвантаження",
                  command=lambda: self._quick_create_outbound()).pack(side='left', padx=10)
        ttk.Button(actions_frame, text="📊 Залишки на складі",
                  command=self._show_reports).pack(side='left', padx=10)
    
    def _quick_create_inbound(self):
        self._show_inbound()
        # Автоматично відкрити діалог створення
        for widget in self.work_area.winfo_children():
            if isinstance(widget, InboundOrderListFrame):
                widget._create()
                break
    
    def _quick_create_outbound(self):
        self._show_outbound()
        for widget in self.work_area.winfo_children():
            if isinstance(widget, OutboundOrderListFrame):
                widget._create()
                break
    
    def _show_items(self):
        self._clear_work_area()
        ItemListFrame(self.work_area, self).pack(fill='both', expand=True)
        self.status_label.config(text="Довідник: Номенклатура")
    
    def _show_clients(self):
        self._clear_work_area()
        ClientListFrame(self.work_area, self).pack(fill='both', expand=True)
        self.status_label.config(text="Довідник: Клієнти 3PL")
    
    def _show_suppliers(self):
        self._clear_work_area()
        SupplierListFrame(self.work_area, self).pack(fill='both', expand=True)
        self.status_label.config(text="Довідник: Постачальники")
    
    def _show_warehouses(self):
        self._clear_work_area()
        WarehouseListFrame(self.work_area, self).pack(fill='both', expand=True)
        self.status_label.config(text="Довідник: Склади")
    
    def _show_zones(self):
        self._clear_work_area()
        ZoneListFrame(self.work_area, self).pack(fill='both', expand=True)
        self.status_label.config(text="Довідник: Зони")
    
    def _show_locations(self):
        self._clear_work_area()
        LocationListFrame(self.work_area, self).pack(fill='both', expand=True)
        self.status_label.config(text="Довідник: Комірки")
    
    def _show_inbound(self):
        self._clear_work_area()
        InboundOrderListFrame(self.work_area, self).pack(fill='both', expand=True)
        self.status_label.config(text="Документи: Приймання")
    
    def _show_outbound(self):
        self._clear_work_area()
        OutboundOrderListFrame(self.work_area, self).pack(fill='both', expand=True)
        self.status_label.config(text="Документи: Відвантаження")
    
    def _show_inventory_count(self):
        self._clear_work_area()
        InventoryCountListFrame(self.work_area, self).pack(fill='both', expand=True)
        self.status_label.config(text="Документи: Інвентаризація")
    
    def _show_reports(self):
        self._clear_work_area()
        ReportsFrame(self.work_area, self).pack(fill='both', expand=True)
        self.status_label.config(text="Звіти")
    
    def _show_users(self):
        if not self.auth_service.has_permission('users_view'):
            messagebox.showerror("Помилка", "Недостатньо прав")
            return
        self._clear_work_area()
        UserListFrame(self.work_area, self).pack(fill='both', expand=True)
        self.status_label.config(text="Адміністрування: Користувачі")
    
    def _show_roles(self):
        if not self.auth_service.has_permission('roles_view'):
            messagebox.showerror("Помилка", "Недостатньо прав")
            return
        self._clear_work_area()
        RoleListFrame(self.work_area, self).pack(fill='both', expand=True)
        self.status_label.config(text="Адміністрування: Ролі")
    
    def _show_audit(self):
        if not self.auth_service.has_permission('audit_view'):
            messagebox.showerror("Помилка", "Недостатньо прав")
            return
        self._clear_work_area()
        AuditLogFrame(self.work_area, self).pack(fill='both', expand=True)
        self.status_label.config(text="Адміністрування: Журнал аудиту")
    
    def _refresh(self):
        """Оновлення поточного вікна"""
        for widget in self.work_area.winfo_children():
            if hasattr(widget, '_load_data'):
                widget._load_data()
                break
    
    def _logout(self):
        """Вихід з системи"""
        if messagebox.askyesno("Підтвердження", "Вийти з системи?"):
            self.auth_service.logout()
            self.destroy()
            # Перезапуск додатку
            python = sys.executable
            os.execl(python, python, *sys.argv)
    
    def _on_close(self):
        """Обробка закриття вікна"""
        if messagebox.askyesno("Підтвердження", "Закрити програму?"):
            self.auth_service.logout()
            self.destroy()


# ============================================================================
# ТОЧКА ВХОДУ
# ============================================================================

def main():
    """Головна функція запуску"""
    try:
        logger.info("=" * 50)
        logger.info(f"Starting {APP_NAME} v{APP_VERSION}")
        logger.info("=" * 50)
        
        # Ініціалізація бази даних
        db = DatabaseManager()
        
        # Запуск додатку
        app = MainApplication()
        app.mainloop()
        
    except Exception as e:
        logger.error(f"Application error: {e}", exc_info=True)
        error_text = f"Помилка запуску програми:\n{e}"
        try:
            messagebox.showerror("Критична помилка", error_text)
        except tk.TclError:
            # Підтримка headless-середовища (без GUI/Display)
            print(error_text, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

        
