import datetime
import os
from flask import Blueprint, request, jsonify
import bcrypt
import jwt
from sqlcipher3 import dbapi2 as sqlite3

auth = Blueprint('authentication', __name__)


JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY")
DB_ENCRYPTION_KEY = os.environ.get("SQLITE_DB_ENCRYPTION_KEY")

ROOT_USERNAME = os.environ.get("ROOT_USERNAME")
ROOT_PASSWORD = os.environ.get("ROOT_PASSWORD")

JWT_ALGORITHM = "HS256"
DB_FILE = "e2e_data_users.db"


def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA key = '{DB_ENCRYPTION_KEY}'")
    return conn


def seed_root_user():
    """Checks if the root user exists. If not, seeds it into the database."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM users WHERE username = ?", (ROOT_USERNAME,))
        row = cursor.fetchone()
        
        if not row:
            print(f"[SEED] Root user '{ROOT_USERNAME}' not found. Seeding root account...")
            
            salt = bcrypt.gensalt()
            hashed_root_password = bcrypt.hashpw(ROOT_PASSWORD.encode("utf-8"), salt)            
            root_permissions = "global_admin,manage_users"
            
            cursor.execute(
                "INSERT INTO users (username, hashed_password, permissions, pasword_changed) VALUES (?, ?, ?, ?)",
                (ROOT_USERNAME, hashed_root_password, root_permissions, 'No')
            )

            conn.commit()
            print("[SEED] Root user seeded successfully.")
        else:
            print("[SEED] Root user already exists. Skipping seed step.")


def init_db():
    """Generates the secure users schema and seeds the administrative user."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                hashed_password BLOB NOT NULL,
                permissions TEXT NOT NULL,
                pasword_changed TEXT
            )
        """)
        conn.commit()

    seed_root_user()

init_db()


def generate_user_jwt(username: str, permissions: list) -> str:
    payload = {
        "sub": username,
        "permissions": permissions,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=2),
        "iat": datetime.datetime.utcnow()
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def require_permission(required_permission: str):
    def decorator(f):
        def wrapper(*args, **kwargs):
            auth_header = request.headers.get("Authorization")
            if not auth_header or not auth_header.startswith("Bearer "):
                return jsonify({"error": "Unauthorized: Missing or malformed Authorization header token."}), 401
            
            token = auth_header.split(" ")[1]
            try:
                payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
                user_permissions = payload.get("permissions", [])
                
                if required_permission not in user_permissions:
                    return jsonify({"error": f"Forbidden: Insufficient privileges. Missing '{required_permission}'."}), 403
                
                request.user_context = payload
                return f(*args, **kwargs)
                
            except jwt.ExpiredSignatureError:
                return jsonify({"error": "Unauthorized: Token session expired. Please log in again."}), 401
            except jwt.InvalidTokenError:
                return jsonify({"error": "Unauthorized: Invalid token modification or invalid signature detected."}), 401
        wrapper.__name__ = f.__name__
        return wrapper
    return decorator


@auth.route("/user/register", methods=["POST"])
def register_user():
    data = request.get_json() or {}
    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        return jsonify({"error": "Bad Request: Username and password strings are mandatory."}), 400

    salt = bcrypt.gensalt()
    hashed_password_bytes = bcrypt.hashpw(password.encode("utf-8"), salt)
    assigned_permissions = ""

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO users (username, hashed_password, permissions) VALUES (?, ?, ?)",
                (username, hashed_password_bytes, assigned_permissions)
            )
            conn.commit()
        return jsonify({"message": "User registered successfully into encrypted SQLite repository."}), 201
    except sqlite3.Error:
        return jsonify({"error": "Conflict: Username already exists or database file write block."}), 400


@auth.route("/user/login", methods=["POST"])
def login_user():
    data = request.get_json() or {}
    username = data.get("username")
    password = data.get("password")

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT username, hashed_password, permissions FROM users WHERE username = ?", (username,))
        row = cursor.fetchone()

    if not row or not bcrypt.checkpw(password.encode("utf-8"), row[1]):
        return jsonify({"error": "Unauthorized: Invalid username or password credentials supplied."}), 401

    permissions_list = row[2].split(",")
    access_token = generate_user_jwt(row[0], permissions_list)

    return jsonify({ "access_token": access_token, "token_type": "Bearer" }), 200


@auth.route("/user/admin/manage", methods=["GET"])
@require_permission("global_admin")
def administrative_endpoint():

    return jsonify({
        "status": "success",
        "message": f"Welcome, {request.user_context.get('sub')}. You have successfully accessed the admin console.",
        "system_users_count": "Protected Data Summary"
    }), 200
