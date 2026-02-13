# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_all
from PyInstaller.utils.hooks import collect_dynamic_libs
from PyInstaller.utils.hooks import collect_submodules
import os

block_cipher = None

app_script = "main.py"   # <-- при необходимости поменяйте

# Собираем ВСЁ по PyQt6: data + binaries + hiddenimports
pyqt6_all = collect_all("PyQt6")

# На всякий случай добираем динамические библиотеки (Qt DLL)
pyqt6_bins = collect_dynamic_libs("PyQt6")

# Плагины Qt (ключевые папки)
# Эти пути будут существовать внутри site-packages PyQt6/Qt6/plugins/...
qt_plugins = []
for p in pyqt6_all[0]:  # datas
    # p: (src, dest)
    qt_plugins.append(p)

datas = []
binaries = []

# Добавляем всё собранное collect_all
datas += pyqt6_all[0]
binaries += pyqt6_all[1]
hiddenimports = pyqt6_all[2]

# Добавляем динамические DLL
binaries += pyqt6_bins

# Ваш файл данных рядом с exe (чтобы текущий код DATA_FILE работал без изменений)
# Если файла может не быть в репо — удалите эти 2 строки и добавляйте его на шаге workflow.
if os.path.exists("tsd_registry_data.json"):
    datas += [("tsd_registry_data.json", ".")]

a = Analysis(
    [app_script],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports + ["pkgutil"],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="TSD_Registry",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,   # windowed
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    name="TSD_Registry",
)
