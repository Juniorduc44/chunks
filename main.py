"""
File Chunker - Version 1.3.0
Modern GUI file splitter using pure default CustomTkinter styling
Appearance mode switcher (Light / Dark / System) + persistence
Last updated: January 2026
"""

import os
import json
import queue
import threading
from pathlib import Path
from typing import List, Optional, Tuple
from enum import Enum
from dataclasses import dataclass
from abc import ABC, abstractmethod

import customtkinter as ctk
from tkinter import filedialog, messagebox

import PyPDF2


# ========================== CONFIG & PERSISTENCE ==========================

CONFIG_FILE = Path.home() / ".file_chunker_config.json"

def load_config():
    default = {"appearance_mode": "System"}
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r") as f:
                return json.load(f)
        except:
            pass
    return default

def save_config(data):
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except:
        pass


# ========================== CORE LOGIC (unchanged from 1.1) ==========================

class SizeUnit(Enum):
    MB = 1024 * 1024
    GB = 1024 * 1024 * 1024
    Mb = 1024 * 1024 // 8
    Gb = 1024 * 1024 * 1024 // 8

    @classmethod
    def from_string(cls, s: str) -> "SizeUnit":
        try:
            return cls[s.upper()]
        except KeyError:
            valid = ', '.join(cls.__members__.keys())
            raise ValueError(f"Invalid unit. Allowed: {valid}") from None


@dataclass(frozen=True)
class ChunkConfig:
    bytes_per_chunk: int | None = None
    number_of_chunks: int | None = None

    def __post_init__(self):
        if self.bytes_per_chunk is None and self.number_of_chunks is None:
            raise ValueError("Must specify either bytes_per_chunk or number_of_chunks")
        if self.bytes_per_chunk is not None and self.bytes_per_chunk <= 1024:
            raise ValueError("Chunk size must be > 1KB")
        if self.number_of_chunks is not None and self.number_of_chunks < 1:
            raise ValueError("Number of parts must be >= 1")


class FileChunker(ABC):
    def __init__(self, config: ChunkConfig):
        self.config = config

    @abstractmethod
    def split(self, src: Path, dst_dir: Path, progress_callback=None) -> List[Path]:
        ...

    @staticmethod
    def _make_chunk_name(original: Path, index: int, total_chunks: int) -> Path:
        idx = f"{index:02d}"
        total_str = f"{total_chunks:02d}" if total_chunks != 0 else "?"
        return original.with_stem(f"{original.stem}_part_{idx}-of-{total_str}")

    def _ensure_output_dir(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        if not os.access(path, os.W_OK):
            raise PermissionError(f"Cannot write to directory: {path}")


class GenericBinaryChunker(FileChunker):
    def split(self, src: Path, dst_dir: Path, progress_callback=None) -> List[Path]:
        if not src.is_file():
            raise FileNotFoundError(f"File not found: {src}")

        self._ensure_output_dir(dst_dir)
        total_size = src.stat().st_size

        if self.config.number_of_chunks is not None:
            parts = max(1, self.config.number_of_chunks)
            bytes_per_chunk = (total_size + parts - 1) // parts
        else:
            bytes_per_chunk = self.config.bytes_per_chunk
            parts = (total_size + bytes_per_chunk - 1) // bytes_per_chunk or 1

        chunks = []
        with open(src, "rb") as f_in:
            for i in range(1, parts + 1):
                chunk_path = dst_dir / self._make_chunk_name(src, i, parts)
                remaining = bytes_per_chunk if i < parts else total_size - f_in.tell()

                with open(chunk_path, "wb") as f_out:
                    data = f_in.read(remaining)
                    if data:
                        f_out.write(data)

                chunks.append(chunk_path)

                if progress_callback:
                    progress_callback(f_in.tell(), total_size, f"Chunk {i}/{parts}")

        return chunks


class TextFileChunker(FileChunker):
    def split(self, src: Path, dst_dir: Path, progress_callback=None) -> List[Path]:
        return GenericBinaryChunker(self.config).split(src, dst_dir, progress_callback)


class PdfFileChunker(FileChunker):
    def split(self, src: Path, dst_dir: Path, progress_callback=None) -> List[Path]:
        if not src.is_file():
            raise FileNotFoundError(src)

        self._ensure_output_dir(dst_dir)

        with open(src, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            total_pages = len(reader.pages)

            if self.config.number_of_chunks is not None:
                n_chunks = max(1, self.config.number_of_chunks)
                pages_per_chunk = (total_pages + n_chunks - 1) // n_chunks
            else:
                pages_per_chunk = max(1, total_pages // 10)

            chunks = []
            page_idx = 0
            chunk_no = 1

            while page_idx < total_pages:
                writer = PyPDF2.PdfWriter()
                start = page_idx
                end = min(page_idx + pages_per_chunk, total_pages)

                for i in range(start, end):
                    writer.add_page(reader.pages[i])

                out_path = dst_dir / self._make_chunk_name(src, chunk_no, n_chunks)
                with open(out_path, "wb") as f_out:
                    writer.write(f_out)

                chunks.append(out_path)
                page_idx = end
                chunk_no += 1

                if progress_callback:
                    progress_callback(page_idx, total_pages, f"Pages {page_idx}/{total_pages}")

            return chunks


def get_chunker_for_file(file_path: Path, config: ChunkConfig) -> FileChunker:
    suffix = file_path.suffix.lower()
    if suffix == ".pdf":
        return PdfFileChunker(config)
    elif suffix in {".txt", ".log", ".csv", ".json", ".md"}:
        return TextFileChunker(config)
    else:
        return GenericBinaryChunker(config)


def estimate_chunks(input_path: Path, config: ChunkConfig) -> Tuple[int, str]:
    total_size = input_path.stat().st_size

    if config.number_of_chunks is not None:
        return config.number_of_chunks, f"{config.number_of_chunks} parts"
    else:
        bytes_per = config.bytes_per_chunk
        estimated = (total_size + bytes_per - 1) // bytes_per
        size_str = f"~{bytes_per / (1024*1024):.1f} MB each"
        return estimated, size_str


# ========================== GUI v1.3.0 ==========================

VERSION = "1.3.0"


class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        config = load_config()
        ctk.set_appearance_mode(config["appearance_mode"])
        ctk.set_default_color_theme("blue")

        self.title(f"File Chunker v{VERSION} • {ctk.get_appearance_mode()}")
        self.geometry("800x840")
        self.resizable(True, True)

        self.queue = queue.Queue()
        self.after(100, self.process_queue)

        self._build_ui()

    def _build_ui(self):
        pad = {"padx": 20, "pady": 10}

        # Header
        header = ctk.CTkFrame(self, fg_color="transparent")
        header.grid(row=0, column=0, columnspan=3, sticky="ew", **pad)
        ctk.CTkLabel(header, text=f"File Chunker v{VERSION}", font=("Segoe UI", 18, "bold")).pack(pady=6)

        # Theme selector
        theme_frame = ctk.CTkFrame(self, fg_color="transparent")
        theme_frame.grid(row=1, column=0, columnspan=3, sticky="ew", **pad)
        ctk.CTkLabel(theme_frame, text="Appearance:").pack(side="left", padx=(0,10))
        self.theme_var = ctk.StringVar(value=ctk.get_appearance_mode())
        for mode in ["System", "Light", "Dark"]:
            ctk.CTkRadioButton(
                theme_frame, text=mode, value=mode,
                variable=self.theme_var, command=self.change_appearance
            ).pack(side="left", padx=12)

        # Input file
        ctk.CTkLabel(self, text="Input File:").grid(row=2, column=0, sticky="w", **pad)
        self.input_entry = ctk.CTkEntry(self, width=560)
        self.input_entry.grid(row=2, column=1, sticky="ew", **pad)
        ctk.CTkButton(self, text="Browse", width=140, command=self.browse_input).grid(row=2, column=2, **pad)

        # Output folder
        ctk.CTkLabel(self, text="Output Folder:").grid(row=3, column=0, sticky="w", **pad)
        self.output_entry = ctk.CTkEntry(self, width=560)
        self.output_entry.grid(row=3, column=1, sticky="ew", **pad)
        ctk.CTkButton(self, text="Browse", width=140, command=self.browse_output).grid(row=3, column=2, **pad)

        # Split mode
        ctk.CTkLabel(self, text="Split Mode:").grid(row=4, column=0, sticky="nw", **pad)
        mode_frame = ctk.CTkFrame(self, fg_color="transparent")
        mode_frame.grid(row=4, column=1, columnspan=2, sticky="w", **pad)

        self.mode_var = ctk.StringVar(value="size")
        ctk.CTkRadioButton(mode_frame, text="By size", variable=self.mode_var,
                           value="size", command=self.update_mode).pack(anchor="w", pady=5)
        ctk.CTkRadioButton(mode_frame, text="By number of parts", variable=self.mode_var,
                           value="parts", command=self.update_mode).pack(anchor="w", pady=5)

        # Size settings
        self.size_frame = ctk.CTkFrame(self)
        self.size_frame.grid(row=5, column=0, columnspan=3, sticky="ew", **pad)
        ctk.CTkLabel(self.size_frame, text="Size:").grid(row=0, column=0, padx=(0,12))
        self.size_entry = ctk.CTkEntry(self.size_frame, width=160)
        self.size_entry.grid(row=0, column=1, padx=8)
        self.size_entry.insert(0, "500")
        self.unit_combo = ctk.CTkComboBox(self.size_frame, values=list(SizeUnit.__members__.keys()), width=160)
        self.unit_combo.grid(row=0, column=2, padx=8)
        self.unit_combo.set("MB")

        # Parts settings
        self.parts_frame = ctk.CTkFrame(self)
        ctk.CTkLabel(self.parts_frame, text="Parts:").grid(row=0, column=0, padx=(0,12))
        self.parts_entry = ctk.CTkEntry(self.parts_frame, width=160)
        self.parts_entry.grid(row=0, column=1, padx=8)
        self.parts_entry.insert(0, "5")

        # Preview
        self.preview_label = ctk.CTkLabel(self, text="Preview: —", text_color="gray")
        self.preview_label.grid(row=6, column=0, columnspan=3, pady=8, sticky="w", padx=20)

        # Progress
        self.progress_bar = ctk.CTkProgressBar(self, width=720)
        self.progress_bar.grid(row=7, column=0, columnspan=3, **pad)
        self.progress_bar.set(0)

        # Log
        ctk.CTkLabel(self, text="Log:").grid(row=8, column=0, sticky="nw", **pad)
        self.log_text = ctk.CTkTextbox(self, height=260, width=720)
        self.log_text.grid(row=9, column=0, columnspan=3, padx=20, pady=(4,20), sticky="nsew")

        # Start button
        self.start_btn = ctk.CTkButton(self, text="Start Splitting", width=260, height=50,
                                       command=self.start_splitting)
        self.start_btn.grid(row=10, column=0, columnspan=3, pady=(20, 40))

        self.grid_columnconfigure(1, weight=1)
        self.update_mode()
        self.update_preview()

    def change_appearance(self):
        mode = self.theme_var.get()
        ctk.set_appearance_mode(mode)
        self.title(f"File Chunker v{VERSION} • {mode}")
        save_config({"appearance_mode": mode})

    def update_mode(self):
        if self.mode_var.get() == "size":
            self.parts_frame.grid_remove()
            self.size_frame.grid()
        else:
            self.size_frame.grid_remove()
            self.parts_frame.grid(row=5, column=0, columnspan=3, sticky="ew", padx=20, pady=10)
        self.update_preview()

    def update_preview(self):
        try:
            p = Path(self.input_entry.get().strip())
            if not p.is_file():
                self.preview_label.configure(text="Preview: — (select file)")
                return

            if self.mode_var.get() == "size":
                val = float(self.size_entry.get())
                unit = SizeUnit.from_string(self.unit_combo.get())
                conf = ChunkConfig(bytes_per_chunk=int(val * unit.value))
            else:
                parts = int(self.parts_entry.get())
                conf = ChunkConfig(number_of_chunks=parts)

            num, desc = estimate_chunks(p, conf)
            size_mb = p.stat().st_size / (1024*1024)
            self.preview_label.configure(text=f"≈ {num} chunks  •  {size_mb:.1f} MB total  •  {desc}")
        except Exception:
            self.preview_label.configure(text="Preview: — (invalid settings)")

    def browse_input(self):
        path = filedialog.askopenfilename()
        if path:
            self.input_entry.delete(0, "end")
            self.input_entry.insert(0, path)
            self.update_preview()

    def browse_output(self):
        path = filedialog.askdirectory()
        if path:
            self.output_entry.delete(0, "end")
            self.output_entry.insert(0, path)

    def log(self, msg: str):
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")

    def process_queue(self):
        try:
            while True:
                cur, tot, msg = self.queue.get_nowait()
                if tot > 0:
                    self.progress_bar.set(cur / tot)
                if msg:
                    self.log(msg)
        except queue.Empty:
            pass
        self.after(100, self.process_queue)

    def start_splitting(self):
        threading.Thread(target=self.split_task, daemon=True).start()

    def split_task(self):
        try:
            inp = Path(self.input_entry.get().strip())
            if not inp.is_file():
                raise ValueError("Input file not found or invalid")

            out = Path(self.output_entry.get().strip())
            if not out:
                raise ValueError("Output directory required")

            mode = self.mode_var.get()

            if mode == "size":
                val = float(self.size_entry.get())
                unit = SizeUnit.from_string(self.unit_combo.get())
                config = ChunkConfig(bytes_per_chunk=int(val * unit.value))
            else:
                parts = int(self.parts_entry.get())
                if parts < 1:
                    raise ValueError("Parts must be ≥ 1")
                config = ChunkConfig(number_of_chunks=parts)

            self.log(f"→ Starting: {inp.name}")
            self.log(f"Mode: {mode}")
            self.progress_bar.set(0)

            chunker = get_chunker_for_file(inp, config)

            def progress(cur: int, tot: int, msg: str = ""):
                self.queue.put((cur, tot, msg))

            chunks = chunker.split(inp, out, progress)

            self.queue.put((1, 1, "\n" + "═" * 70))
            self.queue.put((1, 1, f"Done — Created {len(chunks)} chunk(s)"))
            for c in chunks:
                mb = c.stat().st_size / (1024*1024)
                self.queue.put((1, 1, f"  • {c.name:<60} ({mb:6.2f} MB)"))
            self.queue.put((1, 1, "═" * 70))

            messagebox.showinfo("Success", f"Completed\nCreated {len(chunks)} chunks")

        except Exception as e:
            messagebox.showerror("Error", str(e))
            self.log(f"ERROR: {str(e)}")


if __name__ == "__main__":
    App().mainloop()