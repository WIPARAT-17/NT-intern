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
    """
    เมนคลาสของแอปพลิเคชัน TFTP Auto Backup Dashboard
    จัดการ UI และ Logic ทั้งหมดสำหรับการสำรองและกู้คืนการตั้งค่าอุปกรณ์เครือข่าย
    """
    def __init__(self, root):
        """
        คอนสตรักเตอร์ของคลาส BackupApp.
        ทำการตั้งค่าหน้าต่างหลัก, สไตล์, ตัวแปรเริ่มต้น และสร้างวิดเจ็ต UI.
        """
        self.root = root  # หน้าต่างหลักของ Tkinter
        self.root.title("TFTP Auto Backup Dashboard")  # ตั้งชื่อหน้าต่าง
        self.root.geometry("1000x800")  # กำหนดขนาดเริ่มต้นของหน้าต่าง
        self.setup_styles()  # เรียกใช้เมธอดเพื่อตั้งค่าสไตล์ UI
        self.setup_variables()  # เรียกใช้เมธอดเพื่อตั้งค่าตัวแปรเริ่มต้น
        self.create_widgets()  # เรียกใช้เมธอดเพื่อสร้างวิดเจ็ต UI ทั้งหมด
        self.create_tree_tags()  # เรียกใช้เมธอดเพื่อสร้าง tag สำหรับ Treeview (ใช้ในการเปลี่ยนสีแถว)
        self.reset_summary_counts()  # เรียกใช้เมธอดเพื่อรีเซ็ตตัวนับสรุปผล
        # กำหนดให้แถวที่ 3 (Treeview) และแถวที่ 4 (Shell box - ซึ่งไม่มีในโค้ดนี้แต่มีการเตรียมไว้) สามารถขยายได้
        self.root.grid_rowconfigure(3, weight=1)    # Treeview
        # self.root.grid_rowconfigure(4, weight=1)   # Shell box (commented out as shell box isn't present)
        self.root.grid_columnconfigure(0, weight=1) # กำหนดให้คอลัมน์แรกสามารถขยายได้
        self.restore_file_path = tk.StringVar() # ตัวแปรสำหรับเก็บที่อยู่ไฟล์ที่ใช้กู้คืน (config file)


    def setup_styles(self):
        """
        ตั้งค่าธีมและสไตล์สำหรับวิดเจ็ต Tkinter โดยใช้ ttk.Style.
        กำหนดสีพื้นหลัง สีข้อความ สีปุ่ม และสไตล์ของ Treeview และ ProgressBar.
        """
        # กำหนดโทนสีสำหรับ UI
        self.bg_root = "#1e1e1e"  # สีพื้นหลังหลัก (เข้ม)
        self.bg_frame = "#2b2b2b" # สีพื้นหลังของเฟรม (เข้มขึ้นเล็กน้อย)
        self.fg_text = "#f78fb3"  # สีข้อความ (ชมพูหวาน)
        self.font_main = ("Segoe UI", 10, "bold")  # ฟอนต์หลัก (Segoe UI, ขนาด 10, ตัวหนา)
        self.bg_button = "#303030"  # สีพื้นหลังปุ่ม
        self.bg_button_active = "#505050" # สีพื้นหลังปุ่มเมื่อ active
        self.bg_tree = "#1a1a1a"    # สีพื้นหลัง Treeview
        self.bg_tree_head = "#333333" # สีพื้นหลังหัวตาราง Treeview

        # กำหนดสีสำหรับสถานะต่างๆ
        self.color_success = "#4caf50"  # สีเขียวสำหรับ Success
        self.color_fail = "#f44336"     # สีแดงสำหรับ Fail
        self.color_skipped = "#ff9800"  # สีส้มสำหรับ Skipped
        self.color_error = "#ffc107"    # สีเหลืองสำหรับ Error
        self.color_link = "#03a9f4"     # สีฟ้าสำหรับ Link (ไม่ได้ใช้โดยตรงใน UI นี้)

        self.root.configure(bg=self.bg_root) # กำหนดสีพื้นหลังของหน้าต่างหลัก

        style = ttk.Style(self.root) # สร้างออบเจกต์ Style
        style.theme_use("clam") # ใช้ธีม "clam" ซึ่งเป็นธีมที่ปรับแต่งได้ดี

        # ตั้งค่าสไตล์สำหรับ Scrollbar
        style.configure("Vertical.TScrollbar",
                        background="#2a2a2a",       # สีของแท่งเลื่อน
                        troughcolor="#1e1e1e",      # สีพื้นหลังของร่อง scrollbar
                        bordercolor="#1e1e1e",      # สีขอบ scrollbar
                        arrowcolor="#888888")       # สีลูกศร scrollbar

        style.configure("Horizontal.TScrollbar",
                        background="#2a2a2a",
                        troughcolor="#1e1e1e",
                        bordercolor="#1e1e1e",
                        arrowcolor="#888888")

        # ตั้งค่าสไตล์สำหรับวิดเจ็ตทั่วไป
        style.configure("TFrame", background=self.bg_frame) # สไตล์สำหรับ ttk.Frame
        style.configure("TLabel", background=self.bg_frame, foreground=self.fg_text, font=self.font_main) # สไตล์สำหรับ ttk.Label
        style.configure("TLabelframe", background=self.bg_frame, foreground=self.fg_text, font=self.font_main) # สไตล์สำหรับ ttk.Labelframe
        style.configure("TLabelframe.Label", background=self.bg_frame, foreground=self.fg_text, font=self.font_main) # สไตล์สำหรับ Label ภายใน Labelframe

        style.configure("TButton", background=self.bg_button, foreground=self.fg_text,
                        font=self.font_main, padding=[5, 3]) # สไตล์สำหรับ ttk.Button
        style.map("TButton", background=[("active", self.bg_button_active)]) # เปลี่ยนสีปุ่มเมื่อถูกเมาส์ชี้/คลิก

        style.configure("pastel_green.Horizontal.TProgressbar",
                        troughcolor=self.bg_button, # สีพื้นหลังของ ProgressBar
                        background=self.color_success,  # สีของแท่ง ProgressBar (เขียว)
                        thickness=14) # ความหนาของ ProgressBar
        
        # ตั้งค่าสไตล์สำหรับ Treeview
        style.configure("Treeview",
                        background=self.bg_tree,
                        foreground=self.fg_text,
                        fieldbackground=self.bg_tree,
                        rowheight=24, # ความสูงของแต่ละแถวใน Treeview
                        font=self.font_main)
        style.map("Treeview", background=[("selected", "#444")]) # สีพื้นหลังเมื่อเลือกแถวใน Treeview
        style.configure("Treeview.Heading",
                        background=self.bg_tree_head,
                        foreground=self.fg_text,
                        font=("Segoe UI", 11, "bold")) # สไตล์สำหรับหัวตาราง Treeview

    def setup_variables(self):
        """
        ตั้งค่าตัวแปรเริ่มต้นที่ใช้ในการเชื่อมต่อและเก็บข้อมูลผลลัพธ์.
        """
        self.telnet_host_list = ["172.28.130.46"] # รายการ IP ของ Telnet Host (Gateway สำหรับ SSH)
        self.tftp_server_ip = "10.223.255.60" # IP ของ TFTP Server
        self.telnet_user = self.ssh_user = "csocgov" # Username สำหรับ Telnet และ SSH
        self.telnet_pass = self.ssh_pass = "csocgov.nt" # Password สำหรับ Telnet และ SSH
        self.ssh_ip_list = """
        
        """.strip().splitlines() # รายการ IP ของอุปกรณ์ที่จะสำรองข้อมูล (เริ่มต้นว่างเปล่า)
        self.results = [] # รายการสำหรับเก็บผลลัพธ์การสำรองข้อมูล
        # ตัวแปรสำหรับนับสรุปผล (จะถูกอัปเดตระหว่างการทำงาน)
        self.total_devices = self.online_count = self.offline_count = 0
        self.skipped_total_count = self.success_backup_count = self.failed_backup_count = 0

    def create_widgets(self):
        """
        สร้างและจัดวางวิดเจ็ต UI ทั้งหมดบนหน้าต่างหลัก.
        ประกอบด้วยปุ่มควบคุม, ข้อมูล TFTP Server, ProgressBar, สรุปผล,
        ตารางแสดงผล (Treeview) และส่วนสำหรับกู้คืน Config.
        """
        # === Top Frame: ปุ่มควบคุมหลัก ===
        top = ttk.Frame(self.root, style='TFrame', padding="10 10 10 0")
        top.grid(row=0, column=0, sticky="ew")
        # สร้างปุ่ม Start Backup, Export Result, Add IPs from File
        for i, (text, cmd) in enumerate([
            ("▶ Start Backup", self.run_thread),
            ("Export Result", self.export_results),
            ("➕ Add IPs from File", self.add_ips_from_file),
        ]):
            btn = ttk.Button(top, text=text, command=cmd)
            btn.grid(row=0, column=i, padx=5, sticky="ew")
            setattr(self, ["btn_start","btn_export","btn_add_ips"][i], btn) # กำหนดชื่อตัวแปรให้ปุ่ม

        # === FTP/TFTP Section: ข้อมูล TFTP Server และ ProgressBar ===
        ftp = ttk.Frame(self.root, padding="10 5 10 5", style='TFrame')
        ftp.grid(row=1, column=0, sticky="ew")
        ttk.Label(ftp, text="TFTP Server:", style='TLabel').grid(row=0, column=0, sticky="w")
        self.tftp_ip_var = tk.StringVar(value=self.tftp_server_ip) # ตัวแปรเก็บ IP ของ TFTP Server
        self.tftp_ip_entry = ttk.Entry(ftp, textvariable=self.tftp_ip_var, width=15) # ช่องกรอก IP TFTP
        self.tftp_ip_entry.configure(foreground="black") # กำหนดสีข้อความเป็นสีดำ
        self.tftp_ip_entry.grid(row=0, column=1, sticky="w")
        self.btn_ping_tftp = ttk.Button(ftp, text="Ping", command=self.ping_tftp_server) # ปุ่ม Ping TFTP Server
        self.btn_ping_tftp.grid(row=0, column=2, sticky="w", padx=(5,0))
        self.tftp_status_label = ttk.Label(ftp, text="", style='TLabel', foreground="blue") # Label แสดงสถานะ Ping TFTP
        self.tftp_status_label.grid(row=0, column=3, sticky="w", padx=(10,0))
        ftp.grid_columnconfigure(4, weight=1) # กำหนดให้คอลัมน์ที่ 4 ขยายได้

        self.progress_var = tk.IntVar() # ตัวแปรสำหรับ ProgressBar
        ttk.Progressbar(ftp, orient="horizontal", mode="determinate",
                        variable=self.progress_var, style="pastel_green.Horizontal.TProgressbar")\
            .grid(row=1, column=0, columnspan=5, sticky="ew", pady=(5,0)) # ProgressBar แสดงความคืบหน้า

        self.progress_status_label = ttk.Label(ftp, text="", style='TLabel', font=("Segoe UI", 9, "italic")) # Label แสดงสถานะความคืบหน้า
        self.progress_status_label.grid(row=2, column=0, columnspan=5, sticky="w")
        
        # เพิ่มแสดง Duration
        self.duration_label = ttk.Label(ftp, text="Duration: 0s", style='TLabel', font=("Segoe UI", 9, "italic"))
        self.duration_label.grid(row=3, column=0, columnspan=5, sticky="w", pady=(5,0))

        # === Restore Section: ส่วนสำหรับกู้คืน Config ===
        restore_frame = ttk.LabelFrame(self.root, text="Restore Config to Device", padding="10", style="TLabelframe")
        restore_frame.grid(row=5, column=0, sticky="ew", padx=10, pady=(0, 10))
        restore_frame.grid_columnconfigure(3, weight=1)

        # --- Row 1: IP Input ---
        ttk.Label(restore_frame, text="Restore IP:", style="TLabel").grid(row=0, column=0, sticky="w")
        self.restore_ip_var = tk.StringVar() # ตัวแปรเก็บ IP สำหรับกู้คืน
        self.restore_ip_entry = ttk.Entry(restore_frame, textvariable=self.restore_ip_var, width=20) # ช่องกรอก IP สำหรับกู้คืน
        self.restore_ip_entry.configure(foreground="black")
        self.restore_ip_entry.grid(row=0, column=1, sticky="w", padx=5)

        self.btn_ping_restore = ttk.Button(restore_frame, text="Ping", command=self.ping_restore_ip) # ปุ่ม Ping IP สำหรับกู้คืน
        self.btn_ping_restore.grid(row=0, column=2, sticky="w")
        self.restore_status_label = ttk.Label(restore_frame, text="", width=15, style="TLabel") # Label แสดงสถานะ Ping IP สำหรับกู้คืน
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
        ttk.Button(restore_frame, text="Browse", command=self.browse_restore_file).grid(row=1, column=2, sticky="w", padx=(5, 0)) # ปุ่ม Browse หาไฟล์ config
        self.restore_file_status_label = ttk.Label(
            restore_frame,
            text="",
            style="TLabel",
            font=("Segoe UI", 9, "italic")
        )
        self.restore_file_status_label.grid(row=1, column=3, sticky="w", padx=(10, 0))
        # --- Row 3: Restore Button ---
        self.btn_restore = ttk.Button(restore_frame, text="♻ Restore Config", command=self.run_restore_thread) # ปุ่ม Restore Config
        self.btn_restore.grid(row=2, column=0, columnspan=4, sticky="ew", pady=(10, 0))
        
        # === Summary Section: สรุปผลการสำรองข้อมูล ===
        summary = ttk.LabelFrame(self.root, text="Summary", padding="10", style='TLabelframe')
        summary.grid(row=2, column=0, sticky="ew", padx=10, pady=(5,0))
        summary.grid_columnconfigure(tuple(range(6)), weight=1) # กำหนดให้คอลัมน์ทั้งหมดขยายได้เท่ากัน
        # Label แสดงสรุปผล (Total, Online, Offline, Success, Failed, Skipped)
        labels = [
            ("📊 Total: 0",    'lbl_total',   "#ffffff"),
            ("🟢 Online: 0",  'lbl_online',  "#2ecc71"),
            ("🔴 Offline: 0", 'lbl_offline', "#e74c3c"),
            ("✅ Success: 0", 'lbl_success', "#1abc9c"),
            ("❌ Failed: 0",  'lbl_failed',  "#c0392b"),
            ("⏭️ Skipped: 0", 'lbl_skipped', "#f39c12"),
        ]
        for i,(txt, attr, clr) in enumerate(labels):
            lbl=ttk.Label(summary,text=txt,font=("Segoe UI",9,"bold"),
                          foreground=clr,style='TLabel')
            lbl.grid(row=0,column=i,padx=5,pady=2)
            setattr(self, attr, lbl) # กำหนดชื่อตัวแปรให้ Label

        # === Results Section: ตารางแสดงผลลัพธ์ (Treeview) ===
        res = ttk.LabelFrame(self.root, text="Device Backup Results", padding="10", style='TLabelframe')
        res.grid(row=3, column=0, sticky="nsew", padx=10, pady=(5,10))
        res.grid_rowconfigure(0, weight=1)
        res.grid_columnconfigure(0, weight=1)
        cols=("IP","Ping","Status","Error","Hostname","DateTime") # คอลัมน์สำหรับ Treeview
        self.tree=ttk.Treeview(res,columns=cols,show='headings') # สร้าง Treeview
        # ตั้งค่าหัวคอลัมน์และตำแหน่งข้อความในคอลัมน์
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, anchor="center")
        self.tree.grid(row=0,column=0,sticky="nsew")

        # เพิ่ม Scrollbar สำหรับ Treeview
        self.vscroll = ttk.Scrollbar(res, orient="vertical", command=self.tree.yview)
        self.vscroll.grid(row=0, column=1, sticky="ns")
        self.hscroll = ttk.Scrollbar(res, orient="horizontal", command=self.tree.xview)
        self.hscroll.grid(row=1, column=0, sticky="ew")
        self.tree.configure(yscrollcommand=self.vscroll.set, xscrollcommand=self.hscroll.set)

        # ผูก event เพื่อปรับขนาดคอลัมน์เมื่อ Treeview ถูกปรับขนาด
        self.tree.bind("<Configure>", self.adjust_column_widths)

    def ping_restore_ip(self):
        """
        ฟังก์ชันสำหรับ Ping IP ของอุปกรณ์ที่ต้องการกู้คืน Config.
        อัปเดตสถานะใน restore_status_label.
        """
        ip = self.restore_ip_entry.get()
        if not ip:
            self.restore_status_label.config(text="Please enter IP", foreground=self.color_error)
            print("⚠ Please enter an IP to ping.")
            return

        if self.is_pingable(ip): # เรียกใช้เมธอด is_pingable เพื่อตรวจสอบ
            self.restore_status_label.config(text="🟢 Online", foreground=self.color_success)
            print(f"🟢 {ip} is reachable (Ping OK)")
        else:
            self.restore_status_label.config(text="🔴 Offline", foreground=self.color_fail)
            print(f"🔴 {ip} is unreachable (Ping FAIL)")

    def restore_config_to_device(self, ip):
        """
        ฟังก์ชันสำหรับกู้คืน Config ไปยังอุปกรณ์ที่ระบุผ่าน Telnet.
        ใช้ TFTP เพื่อส่งไฟล์ Config.
        """
        ip = self.restore_ip_entry.get().strip() # IP ของอุปกรณ์เป้าหมาย
        config_file = self.restore_file_path.get().strip() # Path ของไฟล์ Config
        tftp_ip = self.tftp_ip_var.get().strip() # IP ของ TFTP Server

        if not ip or not config_file:
            print("❌ Please fill in both IP and config file.")
            return

        if not os.path.exists(config_file):
            print("❌ Config file not found.")
            return

        config_filename = os.path.basename(config_file) # ชื่อไฟล์ Config (เฉพาะชื่อไฟล์)

        print("=== 🚀 Starting Restore Process ===")
        print(f"🖥 Target Device IP: {ip}")
        print(f"📁 Selected Config File: {config_filename}")
        print(f"🌐 TFTP Server: {tftp_ip}")

        try:
            print(f"🔌 Connecting via Telnet to {ip}...")
            tn = telnetlib.Telnet(ip, timeout=5) # สร้าง Telnet object

            print("🔐 Logging in...")
            tn.read_until(b"username:", timeout=5) # รอ Promp "username:"
            tn.write(self.telnet_user.encode("ascii") + b"\n") # ส่ง username
            tn.read_until(b"Password:", timeout=5) # รอ Prompt "Password:"
            tn.write(self.telnet_pass.encode("ascii") + b"\n") # ส่ง password
            tn.read_until(b"#", timeout=10) # รอ Prompt "#" (แสดงว่า login สำเร็จ)
            print("✅ Telnet Login Success")

            print("⚙ Setting terminal length...")
            tn.write(b"terminal length 0\n") # ตั้งค่า terminal length เพื่อไม่ให้แบ่งหน้า
            tn.read_until(b"#", timeout=3)

            print("📤 Sending restore command: copy tftp: running-config")
            tn.write(b"copy tftp: running-config\n") # ส่งคำสั่ง copy config

            tn.read_until(b"Address or name", timeout=5) # รอ Prompt ให้กรอก IP TFTP
            print(f"📤 Sending TFTP IP: {tftp_ip}")
            tn.write(tftp_ip.encode("ascii") + b"\n") # ส่ง IP TFTP

            tn.read_until(b"filename", timeout=5) # รอ Prompt ให้กรอกชื่อไฟล์
            print(f"📤 Sending config filename: {config_filename}")
            tn.write(config_filename.encode("ascii") + b"\n") # ส่งชื่อไฟล์ Config

            tn.read_until(b"Destination filename", timeout=5) # รอ Prompt ยืนยันชื่อไฟล์ปลายทาง
            print("📤 Confirming destination filename (Enter)")
            tn.write(b"\n") # กด Enter เพื่อยืนยัน

            print("⏳ Waiting for operation to finish...")
            output = tn.read_until(b"#", timeout=20).decode("utf-8", errors="ignore") # รอจนกว่าจะกลับมาที่ Prompt "#"
            tn.close() # ปิดการเชื่อมต่อ Telnet

            print("📦 Device Output:")
            print(output.strip())

            if "copied" in output.lower(): # ตรวจสอบคำว่า "copied" ใน output เพื่อยืนยันความสำเร็จ
                print(f"✅ Restore COMPLETE to {ip}")
            else:
                print(f"❌ Restore FAILED to {ip} – No 'copied' confirmation in output")

        except Exception as e:
            print(f"❌ ERROR during Restore to {ip}: {e}") # ดักจับและแสดงข้อผิดพลาด

        print("=== ✅ Restore Process Finished ===\n")
        
    def run_restore_thread(self):
        """
        เรียกใช้ฟังก์ชัน restore_config_to_device ในเธรดแยกต่างหาก
        เพื่อไม่ให้ UI ค้าง.
        """
        ip = self.restore_ip_var.get().strip()
        if not ip:
            print("❌ No IP provided for restore.")
            return
        # สร้างเธรดใหม่และเริ่มต้นการทำงาน
        threading.Thread(target=self.restore_config_to_device, args=(ip,), daemon=True).start()

    def browse_restore_file(self):
        """
        เปิดหน้าต่าง File Dialog เพื่อให้ผู้ใช้เลือกไฟล์ Config สำหรับกู้คืน.
        อัปเดตชื่อไฟล์ที่เลือกใน UI.
        """
        file_path = filedialog.askopenfilename(
            title="Select Config File",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")] # กำหนดประเภทไฟล์ที่อนุญาต
        )
        if file_path:
            self.restore_file_path.set(file_path) # เก็บ Path เต็มของไฟล์
            filename = file_path.split('/')[-1]  # หรือใช้ os.path.basename
            self.restore_file_label.config(text=filename) # แสดงแค่ชื่อไฟล์ใน Label
            self.restore_file_status_label.config(text="Selected", foreground="green")
        else:
            self.restore_file_label.config(text="No file selected")
            self.restore_file_status_label.config(text="No file selected", foreground="red")
    
    def adjust_column_widths(self, event=None):
        """
        ปรับขนาดความกว้างของคอลัมน์ใน Treeview ให้เหมาะสมกับขนาดของ Treeview.
        """
        w = self.tree.winfo_width() # รับความกว้างปัจจุบันของ Treeview
        total = len(self.tree['columns']) # จำนวนคอลัมน์
        for c in self.tree['columns']:
            self.tree.column(c, width=int(w/total)-2) # กำหนดความกว้างเฉลี่ยให้แต่ละคอลัมน์

    def create_tree_tags(self):
        """
        สร้าง tag สำหรับ Treeview เพื่อใช้ในการเปลี่ยนสีพื้นหลังและสีข้อความของแถว
        ตามสถานะ (Success, Fail, Skipped, Error).
        """
        self.tree.tag_configure("success_row", background="#263A29", foreground=self.color_success) # แถวสำเร็จ
        self.tree.tag_configure("fail_row", background="#3A1E1E", foreground=self.color_fail)     # แถวล้มเหลว
        self.tree.tag_configure("skipped_row", background="#2f2f2f", foreground=self.color_skipped) # แถวที่ข้าม
        self.tree.tag_configure("error_row", background="#3F2F1E", foreground=self.color_error)   # แถวที่มีข้อผิดพลาด

    def is_pingable(self, ip):
        """
        ตรวจสอบว่า IP ที่กำหนดสามารถ Ping ได้หรือไม่.
        รองรับทั้ง Windows และ Linux/macOS.
        """
        param = "-n" if platform.system().lower() == "windows" else "-c" # Parameter สำหรับ Ping (-n for Windows, -c for Linux)
        wait_opt = "-w" if param == "-n" else "-W" # Parameter สำหรับกำหนด timeout
        wait = "1000" if param == "-n" else "1" # ค่า timeout (ms for Windows, s for Linux)

        kwargs = {
            "stdout": subprocess.DEVNULL, # ปิดการแสดงผล standard output
            "stderr": subprocess.DEVNULL  # ปิดการแสดงผล standard error
        }

        if platform.system().lower() == "windows":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW # ซ่อนหน้าต่าง console
            kwargs["startupinfo"] = startupinfo
            kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW # สร้าง process โดยไม่มีหน้าต่าง console

        # เรียกใช้คำสั่ง ping และคืนค่า True หาก Ping สำเร็จ (exit code 0)
        return subprocess.call(["ping", param, "1", wait_opt, wait, ip], **kwargs) == 0

    def connect_and_backup_via_telnet(self, ip):
        """
        เชื่อมต่อกับ Telnet Host (ที่เป็น Gateway) จากนั้น SSH ไปยังอุปกรณ์เป้าหมาย
        และทำการสำรอง running-config ไปยัง TFTP Server.
        """
        for host in self.telnet_host_list: # วนลูปผ่าน Telnet Host ที่กำหนด (กรณีมีหลายตัว)
            try:
                # เชื่อมต่อ Telnet ไปยัง Gateway
                tn = telnetlib.Telnet(host, timeout=5)
                print(tn.read_until(b"username:",timeout=5).decode())
                tn.write((self.telnet_user+"\n").encode())
                print(">>> Username sent")
                print(tn.read_until(b"Password:",timeout=5).decode())
                tn.write((self.telnet_pass+"\n").encode())
                prompt = tn.read_until(b"#",timeout=10).decode()
                print(prompt)

                # SSH จาก Gateway ไปยังอุปกรณ์เป้าหมาย
                tn.write(f"ssh -l {self.ssh_user} {ip}\n".encode())
                auth = tn.read_until(b":",timeout=10).decode(); print(auth)
                # ตรวจสอบข้อความ Error จาก SSH
                if any(err in auth.lower() for err in ["refused","timeout","unknown"]):
                    tn.close(); continue # หากมี Error ให้ปิด Telnet และลอง Host ถัดไป
                tn.write((self.ssh_pass+"\n").encode())
                ssh_prompt = tn.read_until(b"#",timeout=10).decode(); print(ssh_prompt)
                hostname = ssh_prompt.strip().splitlines()[-1].replace("#","").strip() # ดึง hostname จาก prompt
                
                # ทำการสำรอง Config ผ่าน SSH
                tn.write(b"terminal length 0\n"); tn.read_until(b"#",timeout=5) # ตั้งค่า terminal length
                tn.write(b"copy running-config tftp:\n") # ส่งคำสั่ง copy config ไปยัง TFTP
                tn.read_until(b"Address or name of remote host",timeout=10) # รอ Prompt TFTP IP
                tn.write((self.tftp_server_ip+"\n").encode()) # ส่ง IP TFTP Server
                tn.read_until(b"Destination filename",timeout=10); tn.write(b"\n") # รอ Prompt ชื่อไฟล์และกด Enter (ใช้ชื่อไฟล์เริ่มต้น)
                output=tn.read_until(b"#",timeout=20).decode(); print(output) # อ่านผลลัพธ์จนกลับมาที่ Prompt
                tn.close() # ปิดการเชื่อมต่อ Telnet

                # ตรวจสอบสถานะการสำรองข้อมูลจาก output
                return ("✅ SUCCESS","",hostname) if "copied" in output.lower() else ("❌ FAILED","No 'copied'",hostname)
            except Exception as e:
                print(f"[ERROR] {e}") # ดักจับและแสดงข้อผิดพลาด
        return ("❌ FAILED","All hosts failed","") # คืนค่า FAILED หากทุก Telnet Host ล้มเหลว

    def run_backup(self):
        """
        จัดการกระบวนการสำรองข้อมูลทั้งหมด:
        - ปิดใช้งานปุ่มต่างๆ
        - ล้างข้อมูลผลลัพธ์และรีเซ็ตตัวนับ
        - วนลูปผ่านรายการ IP เพื่อ Ping และสำรองข้อมูล
        - อัปเดต UI (ProgressBar, Summary, Treeview) แบบ Thread-safe
        - คำนวณและแสดงผลระยะเวลาที่ใช้ในการสำรองข้อมูล
        """
        self.disable_buttons() # ปิดใช้งานปุ่มต่างๆ เพื่อป้องกันการกดซ้ำ
        self.results.clear() # ล้างผลลัพธ์เก่า
        self.reset_summary_counts() # รีเซ็ตตัวนับสรุปผล
        for r in self.tree.get_children(): self.tree.delete(r) # ล้างข้อมูลใน Treeview
        self.total_devices = len(self.ssh_ip_list) # จำนวนอุปกรณ์ทั้งหมด
        self.update_summary_labels() # อัปเดต Label สรุปผล

        start_time = datetime.now()  # เริ่มจับเวลา
      
        def task(ip):
            """
            ฟังก์ชันย่อยที่ทำงานในแต่ละเธรดสำหรับแต่ละ IP.
            ทำการ Ping และเรียกเมธอดสำรองข้อมูล.
            """
            self.thread_safe_log(f"⏳ Starting backup for {ip}...") # Log ข้อความแบบ Thread-safe
            if self.is_pingable(ip): # ตรวจสอบการ Ping
                self.online_count += 1; self.update_summary_labels() # อัปเดตตัวนับและ UI (Online)
                st, err, hn = self.connect_and_backup_via_telnet(ip) # สำรองข้อมูล
                if "SUCCESS" in st:
                    self.success_backup_count += 1
                    tag = "success_row"
                else:
                    self.failed_backup_count += 1
                    tag = "fail_row"
                self.update_summary_labels() # อัปเดตตัวนับและ UI (Success/Failed)
                return (ip, "✅ Online", st, err, hn, tag)
            else:
                self.offline_count += 1
                self.skipped_total_count += 1
                self.update_summary_labels() # อัปเดตตัวนับและ UI (Offline/Skipped)
                return (ip, "❌ Offline", "⏭️ SKIPPED", "Host unreachable", "", "skipped_row")

        # ใช้ ThreadPoolExecutor เพื่อรันการ Ping และ Backup แบบ Multi-thread
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            completed = 0
            futures = [ex.submit(task, ip) for ip in self.ssh_ip_list] # ส่งแต่ละ IP ให้กับเธรด
            for future in concurrent.futures.as_completed(futures): # เมื่อเธรดทำงานเสร็จ
                ip, ps, st, err, hn, tag = future.result() # รับผลลัพธ์
                nnow = datetime.now()
                datetime_str = nnow.strftime("%Y-%m-%d %H:%M:%S")
                self.results.append((ip, ps, st, err, hn, datetime_str)) # เพิ่มผลลัพธ์เข้ารายการ
                self.tree.insert("", "end", values=(ip, ps, st, err, hn, datetime_str), tags=(tag,)) # เพิ่มแถวใน Treeview

                completed += 1
                self.progress_var.set(int((completed/self.total_devices)*100)) # อัปเดต ProgressBar
                self.progress_status_label.config(text=f"{completed}/{self.total_devices} completed") # อัปเดตสถานะความคืบหน้า
                # อัพเดต Duration
                duration_sec = int((datetime.now() - start_time).total_seconds()) # คำนวณระยะเวลา
                hours, remainder = divmod(duration_sec, 3600)
                minutes, seconds = divmod(remainder, 60)
                self.duration_label.config(text=f"Duration: {hours:02}:{minutes:02}:{seconds:02}")
                self.root.update_idletasks() # บังคับให้ UI อัปเดตทันที

        self.progress_var.set(100) # ตั้ง ProgressBar เป็น 100%
        self.progress_status_label.config(text="Backup finished.") # แสดงสถานะว่าสำรองข้อมูลเสร็จสิ้น
        self.enable_buttons() # เปิดใช้งานปุ่มอีกครั้ง

    def run_thread(self):
        """
        เรียกใช้เมธอด run_backup ในเธรดแยกต่างหาก เพื่อไม่ให้ UI ค้าง
        ระหว่างกระบวนการสำรองข้อมูลที่อาจใช้เวลานาน.
        """
        threading.Thread(target=self.run_backup, daemon=True).start()

    def update_summary_labels(self):
        """
        อัปเดตข้อความใน Label สรุปผล (Total, Online, Offline, Success, Failed, Skipped).
        """
        self.lbl_total.config(text=f"📊 Total: {self.total_devices}")
        self.lbl_online.config(text=f"🟢 Online: {self.online_count}")
        self.lbl_offline.config(text=f"🔴 Offline: {self.offline_count}")
        self.lbl_skipped.config(text=f"⏭️ Skipped: {self.skipped_total_count}")
        self.lbl_success.config(text=f"✅ Success: {self.success_backup_count}")
        self.lbl_failed.config(text=f"❌ Failed: {self.failed_backup_count}")

    def reset_summary_counts(self):
        """
        รีเซ็ตตัวนับสำหรับสรุปผลทั้งหมดให้เป็นศูนย์.
        """
        self.online_count = 0
        self.offline_count = 0
        self.skipped_total_count = 0
        self.success_backup_count = 0
        self.failed_backup_count = 0

    def disable_buttons(self):
        """
        ปิดใช้งานปุ่มควบคุมหลักทั้งหมด เพื่อป้องกันการกดซ้ำระหว่างการทำงาน.
        """
        self.btn_start.config(state="disabled")
        self.btn_export.config(state="disabled")
        self.btn_add_ips.config(state="disabled")

    def enable_buttons(self):
        """
        เปิดใช้งานปุ่มควบคุมหลักทั้งหมด เมื่อการทำงานเสร็จสิ้น.
        """
        self.btn_start.config(state="normal")
        self.btn_export.config(state="normal")
        self.btn_add_ips.config(state="normal")
        
    def thread_safe_log(self, message):
        """
        ฟังก์ชันสำหรับแสดงข้อความ Log ใน Console (หรือพื้นที่ที่กำหนด)
        อย่างปลอดภัยเมื่อมีการเรียกจากเธรดอื่น (ใช้ self.root.after).
        """
        self.root.after(0, print, message)

    def ping_tftp_server(self):
        """
        ฟังก์ชันสำหรับ Ping TFTP Server ที่กำหนด.
        แสดงสถานะการ Ping ใน UI.
        """
        self.btn_ping_tftp.config(state="disabled") # ปิดใช้งานปุ่ม Ping ชั่วคราว
        self.tftp_status_label.config(text="Pinging...") # แสดงสถานะ "Pinging..."
        self.root.update_idletasks() # บังคับให้ UI อัปเดตทันที

        ip = self.tftp_server_ip
        param = "-n" if platform.system().lower() == "windows" else "-c"
        wait_opt = "-w" if param == "-n" else "-W"
        wait = "1000" if param == "-n" else "1"

        # การเรียกใช้ subprocess.call สำหรับ ping
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

        # อัปเดตสถานะใน UI ตามผลการ Ping
        if result == 0:
           self.tftp_status_label.config(text="🟢 Online", foreground=self.color_success)
        else:
            self.tftp_status_label.config(text="🔴 Offline", foreground=self.color_fail)
        self.btn_ping_tftp.config(state="normal") # เปิดใช้งานปุ่ม Ping อีกครั้ง
        
    def add_ips_from_file(self):
        """
        เปิดหน้าต่าง File Dialog เพื่อให้ผู้ใช้เลือกไฟล์ Text ที่มีรายการ IP.
        อ่าน IP จากไฟล์และเพิ่มเข้าไปใน self.ssh_ip_list (ป้องกัน IP ซ้ำ).
        """
        file = filedialog.askopenfilename(title="Select IP List File",
                                          filetypes=[("Text files","*.txt"),("All files","*.*")])
        if file:
            original_count = len(self.ssh_ip_list)
            with open(file, "r") as f:
                for line in f:
                    ip = line.strip()
                    if ip and ip not in self.ssh_ip_list: # เพิ่มเฉพาะ IP ที่ไม่ซ้ำและไม่ว่างเปล่า
                        self.ssh_ip_list.append(ip)
            new_count = len(self.ssh_ip_list) - original_count
            print(f"เพิ่ม IP ใหม่จำนวน {new_count} รายการจากไฟล์.") # แสดงจำนวน IP ที่เพิ่ม

    def export_results(self):
        """
        ส่งออกผลลัพธ์การสำรองข้อมูลที่เก็บไว้ใน self.results ไปยังไฟล์ CSV.
        ผู้ใช้สามารถเลือกตำแหน่งและชื่อไฟล์.
        """
        if not self.results:
            print("[EXPORT] No data to export.")
            return

        # สร้างชื่อไฟล์อัตโนมัติ เช่น results_20250630_1530.csv
        now = datetime.now() # ใช้ datetime.now()
        default_filename = f"results_{now.strftime('%Y%m%d_%H%M%S')}.csv"

        file = filedialog.asksaveasfilename(defaultextension=".csv",
                                            initialfile=default_filename,
                                            filetypes=[("CSV files","*.csv"),("All files","*.*")])
        if file:
            try:
                with open(file, mode="w", newline="", encoding="utf-8-sig") as f:
                    writer = csv.writer(f)
                    writer.writerow(("IP","Ping","Status","Error","Hostname","Date","Time")) # เขียน Header
                    for row in self.results:
                        writer.writerow(row) # เขียนข้อมูลแต่ละแถว
                print(f"[EXPORT] Data exported to {file}")
            except Exception as e:
                print(f"[EXPORT] Failed to export: {e}")


if __name__ == "__main__":
    """
    จุดเริ่มต้นของการรันแอปพลิเคชัน.
    สร้างหน้าต่าง Tkinter และเรียกใช้คลาส BackupApp.
    """
    root = tk.Tk() # สร้างหน้าต่าง Tkinter หลัก
    app = BackupApp(root) # สร้าง instance ของคลาส BackupApp
    root.mainloop() # เริ่มต้น Tkinter event loop (ทำให้ UI แสดงผลและตอบสนองการกระทำของผู้ใช้)
