import threading
import tkinter as tk
from tkinter import scrolledtext, ttk, filedialog
import telnetlib
import csv
import os
import platform
import subprocess
import concurrent.futures
from datetime import datetime

class BackupApp:
    def __init__(self, root):
        self.root = root
        self.root.title("TFTP Auto Backup Dashboard")
        self.root.geometry("1000x800")
        self.setup_styles()
        self.setup_variables()
        self.create_widgets()
        self.create_tree_tags()
        self.reset_summary_counts()
        self.root.grid_rowconfigure(3, weight=1)    # Treeview
        self.root.grid_rowconfigure(4, weight=1)    # Shell box
        self.root.grid_columnconfigure(0, weight=1)
        self.restore_file_path = tk.StringVar()


    def setup_styles(self):
        self.bg_root = "#1e1e1e"
        self.bg_frame = "#2b2b2b"
        self.fg_text = "#f78fb3"  # สีชมพูหวาน
        self.font_main = ("Segoe UI", 10, "bold")  # ตัวหนา
        self.bg_button = "#303030"
        self.bg_button_active = "#505050"
        self.bg_tree = "#1a1a1a"
        self.bg_tree_head = "#333333"

        self.color_success = "#4caf50"
        self.color_fail = "#f44336"
        self.color_skipped = "#ff9800"
        self.color_error = "#ffc107"
        self.color_link = "#03a9f4"

        self.root.configure(bg=self.bg_root)

        style = ttk.Style(self.root)
        style = ttk.Style(self.root)

        # Scrollbar สีเทาอ่อน
        style.configure("Vertical.TScrollbar",
                background="#2a2a2a",       # Scrollbar แถบเลื่อนเข้มมาก
                troughcolor="#1e1e1e",       # พื้นหลังร่อง scrollbar
                bordercolor="#1e1e1e",       # สีขอบให้กลืนกับพื้นหลัง
                arrowcolor="#888888")        # ลูกศรสีเทากลาง

        style.configure("Horizontal.TScrollbar",
                background="#2a2a2a",
                troughcolor="#1e1e1e",
                bordercolor="#1e1e1e",
                arrowcolor="#888888")


        style.theme_use("clam")

        style.configure("TFrame", background=self.bg_frame)
        style.configure("TLabel", background=self.bg_frame, foreground=self.fg_text, font=self.font_main)
        style.configure("TLabelframe", background=self.bg_frame, foreground=self.fg_text, font=self.font_main)
        style.configure("TLabelframe.Label", background=self.bg_frame, foreground=self.fg_text, font=self.font_main)

        style.configure("TButton", background=self.bg_button, foreground=self.fg_text,
                        font=self.font_main, padding=[5, 3])
        style.map("TButton", background=[("active", self.bg_button_active)])

        style.configure("pink.Horizontal.TProgressbar",
                troughcolor=self.bg_button,
                background=self.color_success,  # เขียว
                thickness=14)
        
        style.configure("Treeview",
                        background=self.bg_tree,
                        foreground=self.fg_text,
                        fieldbackground=self.bg_tree,
                        rowheight=24,
                        font=self.font_main)
        style.map("Treeview", background=[("selected", "#444")])
        style.configure("Treeview.Heading",
                        background=self.bg_tree_head,
                        foreground=self.fg_text,
                        font=("Segoe UI", 11, "bold"))  # หัวตารางใหญ่ขึ้น

    def setup_variables(self):
        self.telnet_host_list = ["172.28.130.46"]
        self.tftp_server_ip = "10.223.255.60"
        self.telnet_user = self.ssh_user = "csocgov"
        self.telnet_pass = self.ssh_pass = "csocgov.nt"
        self.ssh_ip_list = """
        
        """.strip().splitlines()
        self.results = []
        self.total_devices = self.online_count = self.offline_count = 0
        self.skipped_total_count = self.success_backup_count = self.failed_backup_count = 0

    def create_widgets(self):
        top = ttk.Frame(self.root, style='TFrame', padding="10 10 10 0")
        top.grid(row=0, column=0, sticky="ew")
        for i, (text, cmd) in enumerate([
            ("▶ Start Backup", self.run_thread),
            ("Export Result", self.export_results),
            ("➕ Add IPs from File", self.add_ips_from_file),
        ]):
            btn = ttk.Button(top, text=text, command=cmd)
            btn.grid(row=0, column=i, padx=5, sticky="ew")
            setattr(self, ["btn_start","btn_export","btn_add_ips"][i], btn)

        ftp = ttk.Frame(self.root, padding="10 5 10 5", style='TFrame')
        ftp.grid(row=1, column=0, sticky="ew")
        ttk.Label(ftp, text="TFTP Server:", style='TLabel').grid(row=0, column=0, sticky="w")
        self.tftp_ip_var = tk.StringVar(value=self.tftp_server_ip)
        self.tftp_ip_entry = ttk.Entry(ftp, textvariable=self.tftp_ip_var, width=15)
        self.tftp_ip_entry.configure(foreground="black")
        self.tftp_ip_entry.grid(row=0, column=1, sticky="w")
        self.btn_ping_tftp = ttk.Button(ftp, text="Ping", command=self.ping_tftp_server)
        self.btn_ping_tftp.grid(row=0, column=2, sticky="w", padx=(5,0))

        self.tftp_status_label = ttk.Label(ftp, text="", style='TLabel', foreground="blue")
        self.tftp_status_label.grid(row=0, column=3, sticky="w", padx=(10,0))

        ftp.grid_columnconfigure(4, weight=1)
        self.progress_var = tk.IntVar()
        ttk.Progressbar(ftp, orient="horizontal", mode="determinate",
                        variable=self.progress_var, style="pastel_green.Horizontal.TProgressbar")\
            .grid(row=1, column=0, columnspan=5, sticky="ew", pady=(5,0))

        self.progress_status_label = ttk.Label(ftp, text="", style='TLabel', font=("Segoe UI", 9, "italic"))
        self.progress_status_label.grid(row=2, column=0, columnspan=5, sticky="w")

        # สร้างตัวแปรสำหรับไฟล์ config
        self.restore_file_path = tk.StringVar()

       # === Restore Section ===
        restore_frame = ttk.LabelFrame(self.root, text="Restore Config to Device", padding="10", style="TLabelframe")
        restore_frame.grid(row=5, column=0, sticky="ew", padx=10, pady=(0, 10))
        restore_frame.grid_columnconfigure(3, weight=1)

        # --- Row 1: IP Input ---
        ttk.Label(restore_frame, text="Restore IP:", style="TLabel").grid(row=0, column=0, sticky="w")
        self.restore_ip_var = tk.StringVar()
        self.restore_ip_entry = ttk.Entry(restore_frame, textvariable=self.restore_ip_var, width=20)
        self.restore_ip_entry.configure(foreground="black")
        self.restore_ip_entry.grid(row=0, column=1, sticky="w", padx=5)

        self.btn_ping_restore = ttk.Button(restore_frame, text="Ping", command=self.ping_restore_ip)
        self.btn_ping_restore.grid(row=0, column=2, sticky="w")

        self.restore_status_label = ttk.Label(restore_frame, text="", width=15, style="TLabel")
        self.restore_status_label.grid(row=0, column=3, sticky="w", padx=(10, 0))

        # --- Row 2: File + Browse ---
        ttk.Label(restore_frame, text="Config File:", style="TLabel").grid(row=1, column=0, sticky="w")

        # แสดงชื่อไฟล์ที่เลือกด้วย Label แทน Entry
        self.restore_file_label = ttk.Label(
            restore_frame,
            text="No file selected",
            style="TLabel",
            width=40,
            anchor="w",
            background="white",
            relief="sunken"
        )
        self.restore_file_label.grid(row=1, column=1, sticky="w", pady=(5, 0))

        ttk.Button(restore_frame, text="Browse", command=self.browse_restore_file).grid(row=1, column=2, sticky="w", padx=(5, 0))

        self.restore_file_status_label = ttk.Label(
            restore_frame,
            text="",
            style="TLabel",
            font=("Segoe UI", 9, "italic")
        )
        self.restore_file_status_label.grid(row=1, column=3, sticky="w", padx=(10, 0))
        # --- Row 3: Restore Button ---
        self.btn_restore = ttk.Button(restore_frame, text="♻ Restore Config", command=self.run_restore_thread)
        self.btn_restore.grid(row=2, column=0, columnspan=4, sticky="ew", pady=(10, 0))
        
                        # เพิ่มแสดง Duration
        self.duration_label = ttk.Label(ftp, text="Duration: 0s", style='TLabel', font=("Segoe UI", 9, "italic"))
        self.duration_label.grid(row=3, column=0, columnspan=5, sticky="w", pady=(5,0))

        summary = ttk.LabelFrame(self.root, text="Summary", padding="10", style='TLabelframe')
        summary.grid(row=2, column=0, sticky="ew", padx=10, pady=(5,0))
        summary.grid_columnconfigure(tuple(range(6)), weight=1)
        labels = [
            ("📊 Total: 0",   'lbl_total',   "#ffffff"),   # ม่วงอมฟ้า
            ("🟢 Online: 0",  'lbl_online',  "#2ecc71"),   # เขียวมะนาว
            ("🔴 Offline: 0", 'lbl_offline', "#e74c3c"),   # แดงสด
            ("✅ Success: 0", 'lbl_success', "#1abc9c"),   # เขียวมิ้นต์
            ("❌ Failed: 0",  'lbl_failed',  "#c0392b"),   # แดงเข้มสด
            ("⏭️ Skipped: 0", 'lbl_skipped', "#f39c12"),   # ส้มทอง
        ]

        for i,(txt, attr, clr) in enumerate(labels):
            lbl=ttk.Label(summary,text=txt,font=("Segoe UI",9,"bold"),
                          foreground=clr,style='TLabel')
            lbl.grid(row=0,column=i,padx=5,pady=2)
            setattr(self, attr, lbl)

        res = ttk.LabelFrame(self.root, text="Device Backup Results", padding="10", style='TLabelframe')
        res.grid(row=3, column=0, sticky="nsew", padx=10, pady=(5,10))
        res.grid_rowconfigure(0, weight=1)
        res.grid_columnconfigure(0, weight=1)
        cols=("IP","Ping","Status","Error","Hostname","DateTime")
        self.tree=ttk.Treeview(res,columns=cols,show='headings')
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, anchor="center")
        self.tree.grid(row=0,column=0,sticky="nsew")

        # แทนที่บรรทัดเดิม
        self.vscroll = ttk.Scrollbar(res, orient="vertical", command=self.tree.yview)
        self.vscroll.grid(row=0, column=1, sticky="ns")

        self.hscroll = ttk.Scrollbar(res, orient="horizontal", command=self.tree.xview)
        self.hscroll.grid(row=1, column=0, sticky="ew")

        self.tree.configure(yscrollcommand=self.vscroll.set, xscrollcommand=self.hscroll.set)

        self.tree.bind("<Configure>", self.adjust_column_widths)

    def ping_restore_ip(self):
        ip = self.restore_ip_entry.get()
        if not ip:
            self.restore_status_label.config(text="Please enter IP", foreground=self.color_error)
            print("⚠ Please enter an IP to ping.")
            return

        if self.is_pingable(ip):
            self.restore_status_label.config(text="🟢 Online", foreground=self.color_success)
            print(f"🟢 {ip} is reachable (Ping OK)")
        else:
            self.restore_status_label.config(text="🔴 Offline", foreground=self.color_fail)
            print(f"🔴 {ip} is unreachable (Ping FAIL)")

    def restore_config_to_device(self, ip):
        ip = self.restore_ip_entry.get().strip()
        config_file = self.restore_file_path.get().strip()
        tftp_ip = self.tftp_ip_var.get().strip()


        if not ip or not config_file:
            print("❌ Please fill in both IP and config file.")
            return

        if not os.path.exists(config_file):
            print("❌ Config file not found.")
            return

        config_filename = os.path.basename(config_file)

        print("=== 🚀 Starting Restore Process ===")
        print(f"🖥 Target Device IP: {ip}")
        print(f"📁 Selected Config File: {config_filename}")
        print(f"🌐 TFTP Server: {tftp_ip}")

        try:
            print(f"🔌 Connecting via Telnet to {ip}...")
            tn = telnetlib.Telnet(ip, timeout=5)

            print("🔐 Logging in...")
            tn.read_until(b"username:", timeout=5)
            tn.write(self.telnet_user.encode("ascii") + b"\n")
            tn.read_until(b"Password:", timeout=5)
            tn.write(self.telnet_pass.encode("ascii") + b"\n")
            tn.read_until(b"#", timeout=10)
            print("✅ Telnet Login Success")

            print("⚙ Setting terminal length...")
            tn.write(b"terminal length 0\n")
            tn.read_until(b"#", timeout=3)

            print("📤 Sending restore command: copy tftp: running-config")
            tn.write(b"copy tftp: running-config\n")

            tn.read_until(b"Address or name", timeout=5)
            print(f"📤 Sending TFTP IP: {tftp_ip}")
            tn.write(tftp_ip.encode("ascii") + b"\n")

            tn.read_until(b"filename", timeout=5)
            print(f"📤 Sending config filename: {config_filename}")
            tn.write(config_filename.encode("ascii") + b"\n")

            tn.read_until(b"Destination filename", timeout=5)
            print("📤 Confirming destination filename (Enter)")
            tn.write(b"\n")

            print("⏳ Waiting for operation to finish...")
            output = tn.read_until(b"#", timeout=20).decode("utf-8", errors="ignore")
            tn.close()

            print("📦 Device Output:")
            print(output.strip())

            if "copied" in output.lower():
                print(f"✅ Restore COMPLETE to {ip}")
            else:
                print(f"❌ Restore FAILED to {ip} – No 'copied' confirmation in output")

        except Exception as e:
            print(f"❌ ERROR during Restore to {ip}: {e}")

        print("=== ✅ Restore Process Finished ===\n")
        
    def run_restore_thread(self):
            ip = self.restore_ip_var.get().strip()
            if not ip:
                print("❌ No IP provided for restore.")
                return
            threading.Thread(target=self.restore_config_to_device, args=(ip,), daemon=True).start()
    def browse_restore_file(self):
        file_path = filedialog.askopenfilename(
            title="Select Config File",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if file_path:
            self.restore_file_path.set(file_path)
            filename = file_path.split('/')[-1]  # หรือใช้ os.path.basename
            self.restore_file_label.config(text=filename)
            self.restore_file_status_label.config(text="Selected", foreground="green")
        else:
            self.restore_file_label.config(text="No file selected")
            self.restore_file_status_label.config(text="No file selected", foreground="red")
    
    def adjust_column_widths(self, event=None):
        w = self.tree.winfo_width()
        total = len(self.tree['columns'])
        for c in self.tree['columns']:
            self.tree.column(c, width=int(w/total)-2)

    def create_tree_tags(self):
        self.tree.tag_configure("success_row", background="#263A29", foreground=self.color_success)
        self.tree.tag_configure("fail_row", background="#3A1E1E", foreground=self.color_fail)
        self.tree.tag_configure("skipped_row", background="#2f2f2f", foreground=self.color_skipped)
        self.tree.tag_configure("error_row", background="#3F2F1E", foreground=self.color_error)

    def is_pingable(self, ip):
        param = "-n" if platform.system().lower() == "windows" else "-c"
        wait_opt = "-w" if param == "-n" else "-W"
        wait = "1000" if param == "-n" else "1"

        kwargs = {
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL
        }

        if platform.system().lower() == "windows":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            kwargs["startupinfo"] = startupinfo
            kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW

        return subprocess.call(["ping", param, "1", wait_opt, wait, ip], **kwargs) == 0

    def connect_and_backup_via_telnet(self, ip):
        for host in self.telnet_host_list:
            try:
                tn = telnetlib.Telnet(host, timeout=5)
                print(tn.read_until(b"username:",timeout=5).decode())
                tn.write((self.telnet_user+"\n").encode())
                print(">>> Username sent")
                print(tn.read_until(b"Password:",timeout=5).decode())
                tn.write((self.telnet_pass+"\n").encode())
                prompt = tn.read_until(b"#",timeout=10).decode()
                print(prompt)
                tn.write(f"ssh -l {self.ssh_user} {ip}\n".encode())
                auth = tn.read_until(b":",timeout=10).decode(); print(auth)
                if any(err in auth.lower() for err in ["refused","timeout","unknown"]):
                    tn.close(); continue
                tn.write((self.ssh_pass+"\n").encode())
                ssh_prompt = tn.read_until(b"#",timeout=10).decode(); print(ssh_prompt)
                hostname = ssh_prompt.strip().splitlines()[-1].replace("#","").strip()
                tn.write(b"terminal length 0\n"); tn.read_until(b"#",timeout=5)
                tn.write(b"copy running-config tftp:\n")
                tn.read_until(b"Address or name of remote host",timeout=10)
                tn.write((self.tftp_server_ip+"\n").encode())
                tn.read_until(b"Destination filename",timeout=10); tn.write(b"\n")
                output=tn.read_until(b"#",timeout=20).decode(); print(output)
                tn.close()
                return ("✅ SUCCESS","",hostname) if "copied" in output.lower() else ("❌ FAILED","No 'copied'",hostname)
            except Exception as e:
                print(f"[ERROR] {e}")
        return ("❌ FAILED","All hosts failed","")

    def run_backup(self):
        self.disable_buttons()
        self.results.clear()
        self.reset_summary_counts()
        for r in self.tree.get_children(): self.tree.delete(r)
        self.total_devices = len(self.ssh_ip_list)
        self.update_summary_labels()

        start_time = datetime.now()  # เริ่มจับเวลา
     
        def task(ip):
            self.thread_safe_log(f"⏳ Starting backup for {ip}...")
            if self.is_pingable(ip):
                self.online_count += 1; self.update_summary_labels()
                st, err, hn = self.connect_and_backup_via_telnet(ip)
                if "SUCCESS" in st:
                    self.success_backup_count += 1
                    tag = "success_row"
                else:
                    self.failed_backup_count += 1
                    tag = "fail_row"
                self.update_summary_labels()
                return (ip, "✅ Online", st, err, hn, tag)
            else:
                self.offline_count += 1
                self.skipped_total_count += 1
                self.update_summary_labels()
                return (ip, "❌ Offline", "⏭️ SKIPPED", "Host unreachable", "", "skipped_row")

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            completed = 0
            futures = [ex.submit(task, ip) for ip in self.ssh_ip_list]
            for future in concurrent.futures.as_completed(futures):
                ip, ps, st, err, hn, tag = future.result()
                nnow = datetime.now()
                datetime_str = nnow.strftime("%Y-%m-%d %H:%M:%S")
                self.results.append((ip, ps, st, err, hn, datetime_str))
                self.tree.insert("", "end", values=(ip, ps, st, err, hn, datetime_str), tags=(tag,))

                completed += 1
                self.progress_var.set(int((completed/self.total_devices)*100))
                self.progress_status_label.config(text=f"{completed}/{self.total_devices} completed")
                # อัพเดต Duration
                duration_sec = int((datetime.now() - start_time).total_seconds())
                hours, remainder = divmod(duration_sec, 3600)
                minutes, seconds = divmod(remainder, 60)
                self.duration_label.config(text=f"Duration: {hours:02}:{minutes:02}:{seconds:02}")
                self.root.update_idletasks()

        self.progress_var.set(100)
        self.progress_status_label.config(text="Backup finished.")
        self.enable_buttons()

    def run_thread(self):
        threading.Thread(target=self.run_backup, daemon=True).start()

    def update_summary_labels(self):
        self.lbl_total.config(text=f"📊 Total: {self.total_devices}")
        self.lbl_online.config(text=f"🟢 Online: {self.online_count}")
        self.lbl_offline.config(text=f"🔴 Offline: {self.offline_count}")
        self.lbl_skipped.config(text=f"⏭️ Skipped: {self.skipped_total_count}")
        self.lbl_success.config(text=f"✅ Success: {self.success_backup_count}")
        self.lbl_failed.config(text=f"❌ Failed: {self.failed_backup_count}")

    def reset_summary_counts(self):
        self.online_count = 0
        self.offline_count = 0
        self.skipped_total_count = 0
        self.success_backup_count = 0
        self.failed_backup_count = 0

    def disable_buttons(self):
        self.btn_start.config(state="disabled")
        self.btn_export.config(state="disabled")
        self.btn_add_ips.config(state="disabled")

    def enable_buttons(self):
        self.btn_start.config(state="normal")
        self.btn_export.config(state="normal")
        self.btn_add_ips.config(state="normal")
        
    def thread_safe_log(self, message):
        self.root.after(0, print, message)

    def ping_tftp_server(self):
        self.btn_ping_tftp.config(state="disabled")
        self.tftp_status_label.config(text="Pinging...")
        self.root.update_idletasks()

        ip = self.tftp_server_ip
        param = "-n" if platform.system().lower() == "windows" else "-c"
        wait_opt = "-w" if param == "-n" else "-W"
        wait = "1000" if param == "-n" else "1"

        if platform.system().lower() == "windows":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            result = subprocess.call(
                ["ping", param, "1", wait_opt, wait, ip],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                startupinfo=startupinfo,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
        else:
            result = subprocess.call(
                ["ping", param, "1", wait_opt, wait, ip],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )

        #self.tftp_status_label.config(text="Online" if result == 0 else "Offline")
        if result == 0:
           self.tftp_status_label.config(text="🟢 Online", foreground=self.color_success)
        else:
            self.tftp_status_label.config(text="🔴 Offline", foreground=self.color_fail)
        self.btn_ping_tftp.config(state="normal")
        
    def add_ips_from_file(self):
        file = filedialog.askopenfilename(title="Select IP List File",
                                          filetypes=[("Text files","*.txt"),("All files","*.*")])
        if file:
            original_count = len(self.ssh_ip_list)
            with open(file, "r") as f:
                for line in f:
                    ip = line.strip()
                    if ip and ip not in self.ssh_ip_list:
                        self.ssh_ip_list.append(ip)
            new_count = len(self.ssh_ip_list) - original_count
            print(f"เพิ่ม IP ใหม่จำนวน {new_count} รายการจากไฟล์.")


    def export_results(self):
        if not self.results:
            print("[EXPORT] No data to export.")
            return

        # สร้างชื่อไฟล์อัตโนมัติ เช่น results_20250630_1530.csv
        now = datetime.datetime.now()
        default_filename = f"results_{now.strftime('%Y%m%d_%H%M%S')}.csv"

        file = filedialog.asksaveasfilename(defaultextension=".csv",
                                            initialfile=default_filename,
                                            filetypes=[("CSV files","*.csv"),("All files","*.*")])
        if file:
            try:
                with open(file, mode="w", newline="", encoding="utf-8-sig") as f:
                    writer = csv.writer(f)
                    writer.writerow(("IP","Ping","Status","Error","Hostname","Date","Time"))
                    for row in self.results:
                        writer.writerow(row)
                print(f"[EXPORT] Data exported to {file}")
            except Exception as e:
                print(f"[EXPORT] Failed to export: {e}")


if __name__ == "__main__":
    root = tk.Tk()
    app = BackupApp(root)
    root.mainloop()








