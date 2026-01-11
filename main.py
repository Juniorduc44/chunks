"""
File Chunker with CustomTkinter GUI
Supports size-based and number-of-parts splitting
"""

from __future__ import annotations

import os
import queue
import threading
from pathlib import Path
from typing import List, Optional

import customtkinter as ctk
from tkinter import filedialog, messagebox

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
        if self.bytes_per_chunk is not None and self.bytes_per_chunk <= 0:
            raise ValueError("Chunk size must be positive")
        if self.number_of_chunks is not None and self.number_of_chunks < 1:
            raise ValueError("Number of parts must be >= 1")


class FileChunker(ABC):
    def __init__(self, config: ChunkConfig):
        self.config = config

    @abstractmethod
    def split(
        self,
        src: Path,
        dst_dir: Path,
        progress_callback=None,
    ) -> List[Path]:
        ...

    @staticmethod
    def _make_chunk_name(original: Path, index: int, total_chunks: int) -> Path:
        idx = f"{index:02d}"
        total_str = f"{total_chunks:02d}"
        new_stem = f"{original.stem}_part_{idx}-of-{total_str}"
        return original.with_stem(new_stem)

    def _ensure_output_dir(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        if not os.access(path, os.W_OK):
            raise PermissionError(f"No write permission: {path}")


class GenericBinaryChunker(FileChunker):
    def split(self, src: Path, dst_dir: Path, progress_callback=None) -> List[Path]:
        if not src.is_file():
            raise FileNotFoundError(src)

        self._ensure_output_dir(dst_dir)
        total_size = src.stat().st_size

        if self.config.number_of_chunks is not None:
            parts = self.config.number_of_chunks
            bytes_per_chunk = (total_size + parts - 1) // parts
        else:
            bytes_per_chunk = self.config.bytes_per_chunk  # type: ignore
            parts = (total_size + bytes_per_chunk - 1) // bytes_per_chunk

        chunks = []
        with open(src, "rb") as f_in:
            for i in range(1, parts + 1):
                chunk_path = dst_dir / self._make_chunk_name(src, i, parts)
                remaining = (
                    bytes_per_chunk if i < parts else total_size - f_in.tell()
                )

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
        # Fallback to binary for now (can be improved later)
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
                n_chunks = self.config.number_of_chunks
                pages_per_chunk = (total_pages + n_chunks - 1) // n_chunks
            else:
                # Rough heuristic for size-based mode
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

                out_path = dst_dir / self._make_chunk_name(src, chunk_no, n_chunks if self.config.number_of_chunks else "??")
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


# ========================== GUI ==========================

class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        ctk.set_appearance_mode("System")
        ctk.set_default_color_theme("blue")

        self.title("File Chunker")
        self.geometry("700x750")
        self.resizable(True, True)

        self.queue = queue.Queue()
        self.after(100, self.process_queue)

        self._build_ui()

    def _build_ui(self):
        pad = dict(padx=10, pady=8)

        # Input file
        ctk.CTkLabel(self, text="Input File:").grid(row=0, column=0, sticky="w", **pad)
        self.input_entry = ctk.CTkEntry(self, width=500)
        self.input_entry.grid(row=0, column=1, **pad)
        ctk.CTkButton(self, text="Browse", command=self.browse_input).grid(row=0, column=2, **pad)

        # Output directory
        ctk.CTkLabel(self, text="Output Directory:").grid(row=1, column=0, sticky="w", **pad)
        self.output_entry = ctk.CTkEntry(self, width=500)
        self.output_entry.grid(row=1, column=1, **pad)
        ctk.CTkButton(self, text="Browse", command=self.browse_output).grid(row=1, column=2, **pad)

        # Mode selection
        ctk.CTkLabel(self, text="Split Mode:").grid(row=2, column=0, sticky="w", **pad)
        self.mode_var = ctk.StringVar(value="size")
        ctk.CTkRadioButton(self, text="By size", variable=self.mode_var, value="size",
                           command=self.update_mode).grid(row=2, column=1, sticky="w")
        ctk.CTkRadioButton(self, text="By number of parts", variable=self.mode_var, value="parts",
                           command=self.update_mode).grid(row=3, column=1, sticky="w")

        # Size mode frame
        self.size_frame = ctk.CTkFrame(self)
        self.size_frame.grid(row=4, column=0, columnspan=3, sticky="ew", **pad)
        ctk.CTkLabel(self.size_frame, text="Chunk size:").grid(row=0, column=0, **pad)
        self.size_entry = ctk.CTkEntry(self.size_frame, width=150)
        self.size_entry.grid(row=0, column=1, **pad)
        self.size_entry.insert(0, "500")
        self.unit_combo = ctk.CTkComboBox(self.size_frame, values=list(SizeUnit.__members__.keys()))
        self.unit_combo.grid(row=0, column=2, **pad)
        self.unit_combo.set("MB")

        # Parts mode frame
        self.parts_frame = ctk.CTkFrame(self)
        ctk.CTkLabel(self.parts_frame, text="Number of parts:").grid(row=0, column=0, **pad)
        self.parts_entry = ctk.CTkEntry(self.parts_frame, width=150)
        self.parts_entry.grid(row=0, column=1, **pad)
        self.parts_entry.insert(0, "5")

        # Progress & log
        self.progress_bar = ctk.CTkProgressBar(self, width=600)
        self.progress_bar.grid(row=5, column=0, columnspan=3, **pad)
        self.progress_bar.set(0)

        ctk.CTkLabel(self, text="Log:").grid(row=6, column=0, sticky="nw", **pad)
        self.log_text = ctk.CTkTextbox(self, height=250)
        self.log_text.grid(row=7, column=0, columnspan=3, sticky="ew", **pad)

        # Start button
        self.start_btn = ctk.CTkButton(self, text="Start Splitting", fg_color="green", hover_color="dark green",
                                       command=self.start_splitting)
        self.start_btn.grid(row=8, column=0, columnspan=3, pady=20)

        self.update_mode()  # initial hide/show

    def update_mode(self):
        if self.mode_var.get() == "size":
            self.parts_frame.grid_remove()
            self.size_frame.grid()
        else:
            self.size_frame.grid_remove()
            self.parts_frame.grid(row=4, column=0, columnspan=3, sticky="ew", padx=10, pady=8)

    def browse_input(self):
        path = filedialog.askopenfilename()
        if path:
            self.input_entry.delete(0, "end")
            self.input_entry.insert(0, path)

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
                messagebox.showerror("Error", "Input file not found or invalid")
                return

            output_dir = Path(self.output_entry.get().strip())
            if not output_dir:
                messagebox.showerror("Error", "Please select an output directory")
                return

            mode = self.mode_var.get()

            if mode == "size":
                try:
                    value = float(self.size_entry.get())
                    unit = SizeUnit.from_string(self.unit_combo.get())
                    config = ChunkConfig(bytes_per_chunk=int(value * unit.value))
                except Exception:
                    messagebox.showerror("Error", "Invalid size value or unit")
                    return
            else:
                try:
                    parts = int(self.parts_entry.get())
                    if parts < 1:
                        raise ValueError
                    config = ChunkConfig(number_of_chunks=parts)
                except Exception:
                    messagebox.showerror("Error", "Invalid number of parts (must be ≥1)")
                    return

            self.log(f"Starting: {input_path.name}")
            self.log(f"Mode: {'size' if mode == 'size' else 'parts'}")
            self.progress_bar.set(0)

            chunker = get_chunker_for_file(input_path, config)

            def progress(current: int, total: int, msg: str = ""):
                self.queue.put((current, total, msg))

            chunks = chunker.split(input_path, output_dir, progress)

            self.queue.put((1, 1, "\n=== Completed ==="))
            self.queue.put((1, 1, f"Created {len(chunks)} chunk(s):"))
            for chunk in chunks:
                size_mb = chunk.stat().st_size / (1024 * 1024)
                self.queue.put((1, 1, f"  • {chunk.name} ({size_mb:.2f} MB)"))

            messagebox.showinfo("Success", f"Splitting complete!\n{len(chunks)} chunks created.")

        except Exception as e:
            messagebox.showerror("Error", str(e))
            self.log(f"Error: {e}")


if __name__ == "__main__":
    App().mainloop()