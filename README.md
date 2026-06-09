# ⚡ MrVpn Workers Panel

<p align="center">
  <img src="https://img.shields.io/badge/Cloudflare-Workers-F38020?style=for-the-badge&logo=Cloudflare&logoColor=white" alt="Cloudflare">
  <img src="https://img.shields.io/badge/SQLite-D1_Database-003B57?style=for-the-badge&logo=SQLite&logoColor=white" alt="SQLite">
  <img src="https://img.shields.io/badge/Status-Active-success?style=for-the-badge" alt="Status">
  <img src="https://img.shields.io/badge/Security-Encrypted_Core-blueviolet?style=for-the-badge" alt="Security">
</p>

یک پنل فوق‌العاده سبک، سریع و ریسپانسیو مبتنی بر **Cloudflare Workers** و دیتابیس ابری **Cloudflare D1** برای مدیریت اسکن هوشمند آی‌پي‌ها و تولید کانفیگ‌های سفارشی VLESS و Trojan به همراه خروجی سابسکریپشن (Subscription).


## ✨ قابلیت‌های کلیدی / Features

- 🖥 **داشبورد مدرن و ریسپانسیو:** طراحی شده با تم ماتریکس و کاملاً بهینه‌سازی شده برای موبایل و دسکتاپ.
- 🗄 **پایگاه داده ابری (D1):** ذخیره دائم و آنی اطلاعات (UUID، پسورد تروجان و آی‌پی‌های تمیز) بدون پاک شدن با بازنشانی ورکر.
- 🔗 **پشتیبانی از Multi-IP:** قابلیت وارد کردن چندین آی‌پی یا دامنه تمیز (جدا شده با کاما `,`) و تولید هم‌زمان کانفیگ‌ها.
- 🛡 **لینک سابسکریپشن اختصاصی:** خروجی Base64 استاندارد در مسیر `/sub` یا `/mrvpn-sub` برای اتصال مستقیم به کلاینت‌ها (v2rayNG, Nekobox و...).
- 💎 **امنیت بالا:** امکان ست کردن نام کاربری و رمز عبور اختصاصی از طریق ابزار Environment Variables کلودفلر.

---

## 🚀 راهنمای نصب و راه‌اندازی سریع / Quick Setup

### مرحله ۱: ساخت دیتابیس D1
1. وارد داشبورد **Cloudflare** شوید.
2. از منوی سمت چپ به مسیر **Workers & Pages** > **D1** بروید.
3. روی **Create database** کلیک کرده و نام آن را `MRVPN_DB` بگذارید.

### مرحله ۲: ساخت Workers و راه‌اندازی کد
1. به بخش **Workers & Pages** برگشته و یک ورکر جدید بسازید (Create Application).
2. کدهای فایل `worker.js` این مخزن را به طور کامل کپی کرده و داخل ادیتور ورکر قرار دهید.

### مرحله ۳: متصل کردن دیتابیس (Binding)
1. در صفحه تنظیمات ورکر خود > **bindings** بروید.
2.  به بخش **D1 Database Bindings** برسید و روی **Add binding** کلیک کنید.
3. نام متغیر (**Variable name**) را دقیقاً برابر با `MRVPN_DB` قرار دهید.
4. دیتابیسی که در مرحله اول ساختید را انتخاب کرده و ذخیره (**Save**) کنید.

### مرحله ۴: تنظیم اطلاعات ورود (اختیاری اما مهم)
در همان تب **Variables**، در بخش **Variables and secret** می‌توانید یوزرنیم و پسورد پنل را شخصی‌سازی کنید:
- متغیر `MR_u` : نام کاربری دلخواه شما (پیش‌فرض: `admin`)
- متغیر `MR_p` : رمز عبور دلخواه شما (پیش‌فرض: `mrvpn123`)

### مرحله ۵: انتشار نهایی
به ادیتور کد برگشته و دکمه **Deploy** را بزنید. اکنون پنل شما آماده است!

---

## 🌐 نحوه دسترسی به پنل / Access Links

- **ورود به پنل مدیریت:**
  `https://your-worker-domain.workers.dev/mrvpn294`
- **لینک سابسکریپشن کلاینت:**
  `https://your-worker-domain.workers.dev/mrvpn-sub`

---

## 🛠 عیب‌یابی (Troubleshooting)

> **ارور خطای لایه بایندینگ (Binding Error):** > اگر با باز کردن صفحه با پیغام "دیتابیس ابری پیدا نشد" مواجه شدید، یعنی مرحله ۳ (D1 Database Binding) را به درستی انجام نداده‌اید یا نام متغیر را دقیقاً `MRVPN_DB` نگذاشته‌اید.

---

## ⚖️ لایسنس و سلب مسئولیت
این پروژه صرفاً جهت تسهیل امور مدیریت شبکه و اهداف آموزشی توسعه یافته است. استفاده نادرست از آن به عهده کاربر می‌باشد.

<p align="center"> Developed with ❤️ by MrVpn Team </p>
