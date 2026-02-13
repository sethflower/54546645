# -*- coding: utf-8 -*-
"""
Реестр ТСД - Программа для ведения учёта терминалов сбора данных
Версия: 2.0 (Tkinter)
"""

import json
import os
import sys
import tkinter as tk
from datetime import datetime
from tkinter import messagebox, simpledialog, ttk

DATA_FILE_NAME = "tsd_registry_data.json"


def get_data_file_path():
    """Возвращает путь к файлу данных рядом с exe/скриптом."""
    if getattr(sys, "frozen", False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, DATA_FILE_NAME)


def load_data():
    default_data = {
        "devices": [],
        "locations": [],
        "statuses": ["В работе", "На складе", "Ремонт"],
        "registry": {},
    }
    data_file = get_data_file_path()
    if os.path.exists(data_file):
        try:
            with open(data_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            for key, value in default_data.items():
                data.setdefault(key, value)
            return data
        except (json.JSONDecodeError, OSError):
            return default_data
    return default_data


def save_data(data):
    data_file = get_data_file_path()
    with open(data_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


class RegistryApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Реестр ТСД")
        self.root.geometry("980x640")
        self.root.minsize(900, 580)

        self.data = load_data()

        self._build_ui()
        self.refresh_all()

    def _build_ui(self):
        style = ttk.Style(self.root)
        if "vista" in style.theme_names():
            style.theme_use("vista")

        notebook = ttk.Notebook(self.root)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)

        self.tab_registry = ttk.Frame(notebook)
        self.tab_dicts = ttk.Frame(notebook)
        notebook.add(self.tab_registry, text="Реестр")
        notebook.add(self.tab_dicts, text="Справочники")

        self._build_registry_tab()
        self._build_dicts_tab()

    def _build_registry_tab(self):
        top = ttk.LabelFrame(self.tab_registry, text="Назначение ТСД")
        top.pack(fill="x", padx=8, pady=8)

        ttk.Label(top, text="Устройство:").grid(row=0, column=0, padx=6, pady=6, sticky="w")
        self.device_cb = ttk.Combobox(top, state="readonly", width=28)
        self.device_cb.grid(row=0, column=1, padx=6, pady=6, sticky="w")

        ttk.Label(top, text="Локация:").grid(row=0, column=2, padx=6, pady=6, sticky="w")
        self.location_cb = ttk.Combobox(top, state="readonly", width=22)
        self.location_cb.grid(row=0, column=3, padx=6, pady=6, sticky="w")

        ttk.Label(top, text="Статус:").grid(row=0, column=4, padx=6, pady=6, sticky="w")
        self.status_cb = ttk.Combobox(top, state="readonly", width=18)
        self.status_cb.grid(row=0, column=5, padx=6, pady=6, sticky="w")

        ttk.Button(top, text="Сохранить", command=self.save_registry_record).grid(row=0, column=6, padx=8, pady=6)
        ttk.Button(top, text="Снять с реестра", command=self.remove_registry_record).grid(row=0, column=7, padx=8, pady=6)

        table_wrap = ttk.LabelFrame(self.tab_registry, text="Текущий реестр")
        table_wrap.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        columns = ("device", "location", "status", "date")
        self.registry_tree = ttk.Treeview(table_wrap, columns=columns, show="headings", height=16)
        self.registry_tree.heading("device", text="Устройство")
        self.registry_tree.heading("location", text="Локация")
        self.registry_tree.heading("status", text="Статус")
        self.registry_tree.heading("date", text="Обновлено")
        self.registry_tree.column("device", width=270)
        self.registry_tree.column("location", width=220)
        self.registry_tree.column("status", width=160)
        self.registry_tree.column("date", width=220)

        yscroll = ttk.Scrollbar(table_wrap, orient="vertical", command=self.registry_tree.yview)
        self.registry_tree.configure(yscrollcommand=yscroll.set)
        self.registry_tree.pack(side="left", fill="both", expand=True, padx=(6, 0), pady=6)
        yscroll.pack(side="right", fill="y", pady=6, padx=(0, 6))

        self.registry_tree.bind("<<TreeviewSelect>>", self.on_registry_select)

    def _build_dicts_tab(self):
        container = ttk.Frame(self.tab_dicts)
        container.pack(fill="both", expand=True, padx=8, pady=8)

        self._build_dictionary_group(container, 0, "Устройства", "devices")
        self._build_dictionary_group(container, 1, "Локации", "locations")
        self._build_dictionary_group(container, 2, "Статусы", "statuses")

        for i in range(3):
            container.columnconfigure(i, weight=1)

    def _build_dictionary_group(self, parent, col, title, key):
        frame = ttk.LabelFrame(parent, text=title)
        frame.grid(row=0, column=col, sticky="nsew", padx=6, pady=6)

        listbox = tk.Listbox(frame, height=20)
        listbox.pack(fill="both", expand=True, padx=6, pady=6)

        btns = ttk.Frame(frame)
        btns.pack(fill="x", padx=6, pady=(0, 6))
        ttk.Button(btns, text="Добавить", command=lambda: self.add_item(key)).pack(side="left", padx=2)
        ttk.Button(btns, text="Изменить", command=lambda: self.edit_item(key)).pack(side="left", padx=2)
        ttk.Button(btns, text="Удалить", command=lambda: self.delete_item(key)).pack(side="left", padx=2)

        setattr(self, f"{key}_listbox", listbox)

    def refresh_all(self):
        self.refresh_dicts()
        self.refresh_comboboxes()
        self.refresh_registry_table()

    def refresh_dicts(self):
        for key in ("devices", "locations", "statuses"):
            lb: tk.Listbox = getattr(self, f"{key}_listbox")
            lb.delete(0, tk.END)
            for item in self.data[key]:
                lb.insert(tk.END, item)

    def refresh_comboboxes(self):
        self.device_cb["values"] = self.data["devices"]
        self.location_cb["values"] = self.data["locations"]
        self.status_cb["values"] = self.data["statuses"]

    def refresh_registry_table(self):
        for row in self.registry_tree.get_children():
            self.registry_tree.delete(row)

        for device in sorted(self.data["registry"].keys()):
            record = self.data["registry"][device]
            self.registry_tree.insert(
                "",
                tk.END,
                iid=device,
                values=(
                    device,
                    record.get("location", ""),
                    record.get("status", ""),
                    record.get("date", ""),
                ),
            )

    def add_item(self, key):
        title = {"devices": "устройство", "locations": "локацию", "statuses": "статус"}[key]
        value = simpledialog.askstring("Добавить", f"Введите {title}:", parent=self.root)
        if not value:
            return
        value = value.strip()
        if not value:
            return
        if value in self.data[key]:
            messagebox.showwarning("Внимание", "Запись уже существует.")
            return
        self.data[key].append(value)
        save_data(self.data)
        self.refresh_all()

    def edit_item(self, key):
        lb: tk.Listbox = getattr(self, f"{key}_listbox")
        sel = lb.curselection()
        if not sel:
            messagebox.showinfo("Информация", "Выберите запись для изменения.")
            return

        old_value = lb.get(sel[0])
        new_value = simpledialog.askstring("Изменить", "Новое значение:", initialvalue=old_value, parent=self.root)
        if not new_value:
            return
        new_value = new_value.strip()
        if not new_value:
            return
        if new_value != old_value and new_value in self.data[key]:
            messagebox.showwarning("Внимание", "Такая запись уже существует.")
            return

        idx = self.data[key].index(old_value)
        self.data[key][idx] = new_value

        if key == "devices" and old_value in self.data["registry"]:
            self.data["registry"][new_value] = self.data["registry"].pop(old_value)
        elif key == "locations":
            for device, rec in self.data["registry"].items():
                if rec.get("location") == old_value:
                    rec["location"] = new_value
                    rec["date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elif key == "statuses":
            for device, rec in self.data["registry"].items():
                if rec.get("status") == old_value:
                    rec["status"] = new_value
                    rec["date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        save_data(self.data)
        self.refresh_all()

    def delete_item(self, key):
        lb: tk.Listbox = getattr(self, f"{key}_listbox")
        sel = lb.curselection()
        if not sel:
            messagebox.showinfo("Информация", "Выберите запись для удаления.")
            return

        value = lb.get(sel[0])
        if not messagebox.askyesno("Подтверждение", f"Удалить: {value}?"):
            return

        self.data[key].remove(value)

        if key == "devices":
            self.data["registry"].pop(value, None)
        elif key == "locations":
            for rec in self.data["registry"].values():
                if rec.get("location") == value:
                    rec["location"] = ""
        elif key == "statuses":
            for rec in self.data["registry"].values():
                if rec.get("status") == value:
                    rec["status"] = ""

        save_data(self.data)
        self.refresh_all()

    def save_registry_record(self):
        device = self.device_cb.get().strip()
        location = self.location_cb.get().strip()
        status = self.status_cb.get().strip()

        if not device:
            messagebox.showwarning("Внимание", "Выберите устройство.")
            return
        if not location:
            messagebox.showwarning("Внимание", "Выберите локацию.")
            return
        if not status:
            messagebox.showwarning("Внимание", "Выберите статус.")
            return

        self.data["registry"][device] = {
            "location": location,
            "status": status,
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        save_data(self.data)
        self.refresh_registry_table()

    def remove_registry_record(self):
        sel = self.registry_tree.selection()
        if not sel:
            messagebox.showinfo("Информация", "Выберите строку в реестре.")
            return
        device = sel[0]
        if messagebox.askyesno("Подтверждение", f"Снять устройство {device} с реестра?"):
            self.data["registry"].pop(device, None)
            save_data(self.data)
            self.refresh_registry_table()

    def on_registry_select(self, _event):
        sel = self.registry_tree.selection()
        if not sel:
            return
        device = sel[0]
        record = self.data["registry"].get(device, {})

        self.device_cb.set(device)
        self.location_cb.set(record.get("location", ""))
        self.status_cb.set(record.get("status", ""))


def main():
    root = tk.Tk()
    app = RegistryApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
