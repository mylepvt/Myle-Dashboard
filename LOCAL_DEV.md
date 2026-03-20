# Local development — bina production credentials ke chalana

## Problem
Local pe app nahi chala rahe kyunki login id/password nahi pata (production DB/credentials nahi hain).

---

## Solution 1: Bypass login (sabse aasaan)

**Login bilkul mat karo** — env se auto admin session:

```bash
DEV_BYPASS_AUTH=1 python app.py
```

Browser mein **http://localhost:5001** kholo → seedha app khul jayegi, koi login page nahi. Session automatically **admin** set ho jati hai.

- Pehli baar run pe ensure karo **admin user exist kare** (fresh DB pe app startup pe `seed_users()` khud admin bana deti hai).
- Production / Render pe **kabhi** `DEV_BYPASS_AUTH` mat set karna.

---

## Solution 2: Fresh DB + login admin / admin123

App **pehli baar** jab run hoti hai aur **users table empty** hota hai, tab automatically ek default admin account ban jata hai:

| Username | Password |
|----------|----------|
| **admin** | **admin123** |

### Steps (Mac/Linux)

1. **Purani DB hatao** (taaki seed_users() naya admin bana sake):
   ```bash
   rm -f leads.db
   ```

2. **App chalao** (tables + admin auto create honge):
   ```bash
   python app.py
   ```
   Ya: `flask run` (agar FLASK_APP=app.py set ho).

3. Browser mein open karo: **http://localhost:5001** (ya jo port dikhe terminal pe).

4. Login: **admin** / **admin123**

### Windows (PowerShell / CMD)

```powershell
del leads.db
python app.py
```

Phir http://localhost:5001 pe jao, login **admin** / **admin123**.

---

## Optional: Sirf local ke liye alag DB file

Production `leads.db` ko touch kiye bina local testing ke liye alag file use kar sakte ho:

```bash
export DATABASE_PATH=./leads_local.db
python app.py
```

Pehli run pe `leads_local.db` create hoga aur usme admin/admin123 seed ho jayega. Production `leads.db` same rahegi.

---

## Note

- **Production / Render** pe kabhi default admin/admin123 rely mat karna — wahan proper users + strong passwords honi chahiye.
- Local pe SECRET_KEY env var optional hai (app fallback use karti hai); production pe zaroor set karo.
