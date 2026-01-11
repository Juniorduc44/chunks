"""
File Chunker - Version 1.1
Modern GUI file splitter using pure default CustomTkinter styling
Supports size-based and number-of-parts splitting
Last updated: January 2025 / 2026
"""

# Standard library
import os
import queue
import threading
from pathlib import Path
from typing import List, Optional, Tuple
from enum import Enum
from dataclasses import dataclass
from abc import ABC, abstractmethod

# GUI & dialogs
import customtkinter as ctk
from tkinter import filedialog, messagebox

# PDF support
import PyPDF2


# ========================== CORE LOGIC ==========================

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
    """Quick preview of how many chunks will be created"""
    total_size = input_path.stat().st_size

    if config.number_of_chunks is not None:
        return config.number_of_chunks, f"{config.number_of_chunks} parts (requested)"
    else:
        bytes_per = config.bytes_per_chunk
        estimated = (total_size + bytes_per - 1) // bytes_per
        size_str = f"~{bytes_per / (1024*1024):.1f} MB each"
        return estimated, size_str


# ========================== GUI ==========================

VERSION = "1.1"


class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        ctk.set_appearance_mode("System")
        ctk.set_default_color_theme("blue")

        self.title(f"File Chunker v{VERSION}")
        self.geometry("780x820")
        self.resizable(True, True)

        self.queue = queue.Queue()
        self.after(100, self.process_queue)

        self._build_ui()

    def _build_ui(self):
        pad = {"padx": 18, "pady": 9}

        # Header / version
        header_frame = ctk.CTkFrame(self, fg_color="transparent")
        header_frame.grid(row=0, column=0, columnspan=3, sticky="ew", **pad)
        ctk.CTkLabel(header_frame, text=f"File Chunker • v{VERSION}", font=("Segoe UI", 16, "bold")).pack(pady=6)

        # Input file
        ctk.CTkLabel(self, text="Input File:").grid(row=1, column=0, sticky="w", **pad)
        self.input_entry = ctk.CTkEntry(self, width=540)
        self.input_entry.grid(row=1, column=1, sticky="ew", **pad)
        ctk.CTkButton(self, text="Browse", width=130, command=self.browse_input).grid(row=1, column=2, **pad)

        # Output folder
        ctk.CTkLabel(self, text="Output Folder:").grid(row=2, column=0, sticky="w", **pad)
        self.output_entry = ctk.CTkEntry(self, width=540)
        self.output_entry.grid(row=2, column=1, sticky="ew", **pad)
        ctk.CTkButton(self, text="Browse", width=130, command=self.browse_output).grid(row=2, column=2, **pad)

        # Mode
        ctk.CTkLabel(self, text="Split Mode:").grid(row=3, column=0, sticky="nw", **pad)
        mode_frame = ctk.CTkFrame(self, fg_color="transparent")
        mode_frame.grid(row=3, column=1, columnspan=2, sticky="w", **pad)

        self.mode_var = ctk.StringVar(value="size")
        ctk.CTkRadioButton(mode_frame, text="By size (MB/GB/etc)", variable=self.mode_var,
                           value="size", command=self.update_mode).pack(anchor="w", pady=4)
        ctk.CTkRadioButton(mode_frame, text="By number of parts", variable=self.mode_var,
                           value="parts", command=self.update_mode).pack(anchor="w", pady=4)

        # Size settings
        self.size_frame = ctk.CTkFrame(self)
        self.size_frame.grid(row=4, column=0, columnspan=3, sticky="ew", **pad)
        ctk.CTkLabel(self.size_frame, text="Size:").grid(row=0, column=0, padx=(0,10))
        self.size_entry = ctk.CTkEntry(self.size_frame, width=150)
        self.size_entry.grid(row=0, column=1, padx=8)
        self.size_entry.insert(0, "500")

        self.unit_combo = ctk.CTkComboBox(self.size_frame, values=list(SizeUnit.__members__.keys()), width=150)
        self.unit_combo.grid(row=0, column=2, padx=8)
        self.unit_combo.set("MB")

        # Parts settings
        self.parts_frame = ctk.CTkFrame(self)
        ctk.CTkLabel(self.parts_frame, text="Number of parts:").grid(row=0, column=0, padx=(0,10))
        self.parts_entry = ctk.CTkEntry(self.parts_frame, width=150)
        self.parts_entry.grid(row=0, column=1, padx=8)
        self.parts_entry.insert(0, "5")

        # Preview label
        self.preview_label = ctk.CTkLabel(self, text="Preview: —", text_color="gray")
        self.preview_label.grid(row=5, column=0, columnspan=3, pady=(4,12), sticky="w", padx=18)

        # Progress
        self.progress_bar = ctk.CTkProgressBar(self, mode="determinate", width=700)
        self.progress_bar.grid(row=6, column=0, columnspan=3, **pad)
        self.progress_bar.set(0)

        # Log
        ctk.CTkLabel(self, text="Progress / Log:").grid(row=7, column=0, sticky="nw", **pad)
        self.log_text = ctk.CTkTextbox(self, height=240, width=700)
        self.log_text.grid(row=8, column=0, columnspan=3, padx=18, pady=(4,18), sticky="nsew")

        # Start button
        self.start_btn = ctk.CTkButton(
            self,
            text="Start Splitting",
            width=240,
            height=48,
            command=self.start_splitting
        )
        self.start_btn.grid(row=9, column=0, columnspan=3, pady=(16, 32))

        self.grid_columnconfigure(1, weight=1)
        self.update_mode()

        # Initial preview update
        self.update_preview()

    def update_mode(self):
        if self.mode_var.get() == "size":
            self.parts_frame.grid_remove()
            self.size_frame.grid()
        else:
            self.size_frame.grid_remove()
            self.parts_frame.grid(row=4, column=0, columnspan=3, sticky="ew", padx=18, pady=9)
        self.update_preview()

    def update_preview(self):
        try:
            path = Path(self.input_entry.get().strip())
            if not path.is_file():
                self.preview_label.configure(text="Preview: — (select a file first)")
                return

            if self.mode_var.get() == "size":
                value = float(self.size_entry.get())
                unit = SizeUnit.from_string(self.unit_combo.get())
                config = ChunkConfig(bytes_per_chunk=int(value * unit.value))
            else:
                parts = int(self.parts_entry.get())
                config = ChunkConfig(number_of_chunks=parts)

            num, desc = estimate_chunks(path, config)
            size_mb = path.stat().st_size / (1024*1024)
            self.preview_label.configure(
                text=f"Preview: ~{num} chunks  |  Original: {size_mb:.1f} MB  |  {desc}"
            )
        except:
            self.preview_label.configure(text="Preview: — (invalid input)")

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

    def log(self, message: str):
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")

    def process_queue(self):
        try:
            while True:
                current, total, msg = self.queue.get_nowait()
                if total > 0:
                    self.progress_bar.set(current / total)
                if msg:
                    self.log(msg)
        except queue.Empty:
            pass
        self.after(100, self.process_queue)

    def start_splitting(self):
        threading.Thread(target=self.split_task, daemon=True).start()

    def split_task(self):
        try:
            input_path = Path(self.input_entry.get().strip())
            if not input_path.is_file():
                raise ValueError("Input file not found or invalid")

            output_dir = Path(self.output_entry.get().strip())
            if not output_dir:
                raise ValueError("Please select an output directory")

            mode = self.mode_var.get()

            if mode == "size":
                value = float(self.size_entry.get())
                unit = SizeUnit.from_string(self.unit_combo.get())
                config = ChunkConfig(bytes_per_chunk=int(value * unit.value))
            else:
                parts = int(self.parts_entry.get())
                if parts < 1:
                    raise ValueError("Number of parts must be at least 1")
                config = ChunkConfig(number_of_chunks=parts)

            self.log(f"Starting: {input_path.name}")
            self.log(f"Mode: {mode}")
            self.progress_bar.set(0)
            self.log_text.configure(state="normal")

            chunker = get_chunker_for_file(input_path, config)

            def progress(current: int, total: int, msg: str = ""):
                self.queue.put((current, total, msg))

            chunks = chunker.split(input_path, output_dir, progress)

            self.queue.put((1, 1, "\n" + "═" * 70))
            self.queue.put((1, 1, f"Successfully created {len(chunks)} chunks"))
            for chunk in chunks:
                size_mb = chunk.stat().st_size / (1024 * 1024)
                self.queue.put((1, 1, f"  • {chunk.name:<60} ({size_mb:6.2f} MB)"))
            self.queue.put((1, 1, "═" * 70))

            messagebox.showinfo("Success", f"Completed!\nCreated {len(chunks)} chunks.")

        except Exception as e:
            messagebox.showerror("Error", str(e))
            self.log(f"ERROR: {str(e)}")


if __name__ == "__main__":
    App().mainloop()