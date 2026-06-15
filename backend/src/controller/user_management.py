from datetime import datetime, timedelta
import os
import asyncio
import functools
from flask import Blueprint, request, jsonify
import bcrypt
import jwt
import aiosqlite
from asgiref.sync import async_to_sync
from contextlib import asynccontextmanager

user_management = Blueprint('authentication', __name__)

JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY", "your-secret-key")
DB_ENCRYPTION_KEY = os.environ.get("SQLITE_DB_ENCRYPTION_KEY", "your-db-key")
ROOT_USERNAME = os.environ.get("ROOT_USERNAME", "root")
ROOT_PASSWORD = os.environ.get("ROOT_PASSWORD", "rootpass")
NAMESPACE = os.environ.get('USER_NAMESPACE', "default")

JWT_ALGORITHM = "HS256"
DB_FILE = "e2e_data_users.db"


async def _get_all_users_async():
    async with get_db_connection() as conn:
        async with conn.execute('SELECT id, username, permissions, password_changed, tenant_name, expire_date, email FROM users') as cursor:
            rows, users = await cursor.fetchall(), []
        
        for r in rows:
            users.append({ 'usr': r[1], 'permissions': r[2].split(',') if r[2] else [], 'pwd_chgd': r[3], 'tenant_name': r[4], 'pwd_exp': r[5], 'email': r[6] })

        return users


async def _save_role_async(role_name: str, description: str):
    async with get_db_connection() as conn:
        await conn.execute("INSERT INTO roles (role_name, description) VALUES (?, ?)", (role_name, description))
        await conn.commit()


async def _save_permission_async(perm_name: str, description: str):
    async with get_db_connection() as conn:
        await conn.execute("INSERT INTO permissions (perm_name, description) VALUES (?, ?)", (perm_name, description))
        await conn.commit()


async def _get_unified_rbac_catalog_async():
    async with get_db_connection() as conn:
        query = """
            SELECT 'role' AS entity_type, id, role_name AS name, description  FROM roles
            UNION ALL
            SELECT 'permission' AS entity_type, id, perm_name AS name, description  FROM permissions
        """
        async with conn.execute(query) as cursor:
            rows = await cursor.fetchall()
        
        catalog = {"roles": [], "permissions": []}
        for r in rows:
            entity_type, entity_id, name, desc = r[0], r[1], r[2], r[3]
            item = {"id": entity_id, "name": name, "description": desc}
            
            if entity_type == 'role': catalog["roles"].append(item)
            else: catalog["permissions"].append(item)

        return catalog


@asynccontextmanager
async def get_db_connection():
    conn = await aiosqlite.connect(DB_FILE)
    try:
        await conn.execute(f"PRAGMA key = '{DB_ENCRYPTION_KEY}'")
        yield conn
    finally:
        await conn.close()


async def seed_root_user():
    async with get_db_connection() as conn:
        async with conn.execute('SELECT id FROM users WHERE username = ?', (ROOT_USERNAME,)) as cursor:
            row = await cursor.fetchone()
        
        if not row:
            loop = asyncio.get_running_loop()
            salt = await loop.run_in_executor(None, bcrypt.gensalt)
            hashed_root_password = await loop.run_in_executor(None, lambda: bcrypt.hashpw(ROOT_PASSWORD.encode("utf-8"), salt))            
            root_permissions = 'global_admin,manage_users'
            
            await conn.execute(
                'INSERT INTO users (username, hashed_password, permissions, password_changed, tenant_name, email) VALUES (?, ?, ?, ?, ?, ?)',
                (ROOT_USERNAME, hashed_root_password, root_permissions, 'No', NAMESPACE, f'{ROOT_USERNAME}@{NAMESPACE}')
            )
            await conn.commit()
            print("[SEED] Root user seeded successfully.")
        else:
            print("[SEED] Root user already exists. Skipping seed step.")


async def seed_rbac():
    base_roles = [
        ('global_admin', 'Full administrative access'), 
        ('analyst', 'Have full access to the to the DW for deep analysis'),
        ('manage_users', 'Runs user management operation like create, add/revoke permission, deactivate and delete'),    
    ]
    base_permissions = [
        ('create:pipeline', 'Ability to create new data pipeline'),
        ('view:pipeline', 'View previously created pipelines'),
        ('schedule:pipeline', 'Schedule pipeline that was previously created')
    ]

    async with get_db_connection() as conn:
        for r_name, r_desc in base_roles:
            await conn.execute("INSERT OR IGNORE INTO roles (role_name, description) VALUES (?, ?)", (r_name, r_desc))
            
        for p_name, p_desc in base_permissions:
            await conn.execute("INSERT OR IGNORE INTO permissions (perm_name, description) VALUES (?, ?)", (p_name, p_desc))
            
        await conn.commit()


async def init_db():
    async with get_db_connection() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                username TEXT UNIQUE NOT NULL, 
                email TEXT UNIQUE NOT NULL,
                hashed_password BLOB NOT NULL, 
                permissions TEXT NOT NULL, 
                password_changed TEXT, 
                tenant_name TEXT, expire_date TEXT
            )
        """)
        await conn.execute("CREATE TABLE IF NOT EXISTS roles (id INTEGER PRIMARY KEY AUTOINCREMENT, role_name TEXT UNIQUE NOT NULL, description TEXT)")
        await conn.execute("CREATE TABLE IF NOT EXISTS permissions (id INTEGER PRIMARY KEY AUTOINCREMENT, perm_name TEXT UNIQUE NOT NULL, description TEXT)")
        await conn.commit()

    await seed_rbac()
    await seed_root_user()

def generate_user_jwt(username: str, permissions: list) -> str:
    payload = {'sub': username, 'permissions': permissions, 'exp': datetime.utcnow() + timedelta(hours=24), 'iat': datetime.utcnow()}
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


def require_permission(required_permission: str):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            auth_header = request.headers.get("Authorization")
            if not auth_header or not auth_header.startswith("Bearer "):
                return {'message': 'Unauthorized: Missing or malformed Authorization header token.', 'error': True}, 401
            
            token = auth_header.split(" ")[1]
            try:
                payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
                
                if required_permission not in payload.get("permissions", []):
                    return {'message': f"Forbidden: Insufficient privileges. Missing '{required_permission}'.", 'error': True}, 403
                
                request.user_context = payload                
                return f(*args, **kwargs)
                
            except jwt.ExpiredSignatureError:
                return {'message': 'Unauthorized: Token session expired. Please log in again.', 'error': True}, 401
            except jwt.InvalidTokenError:
                return {'message': 'Unauthorized: Invalid token modification or invalid signature detected.', 'error': True}, 401
        return wrapper
    return decorator


@user_management.route("/user", methods=["POST"])
def register_user():
    data = request.get_json() or {}
    username, password, email = data.get('username'), data.get('password'), data.get('email')

    if not username or not password:
        return jsonify({'error': 'Bad Request: Username and password strings are mandatory.'}), 400

    async def async_register():
        loop = asyncio.get_running_loop()
        hashed_pwd = await loop.run_in_executor(None, lambda: bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()))
        permissions = ''

        try:
            async with get_db_connection() as conn:
                await conn.execute(
                    'INSERT INTO users (username, hashed_password, permissions, email) VALUES (?, ?, ?, ?)', (username, hashed_pwd, permissions, email)
                )
                await conn.commit()
            return {'message': 'User registered successfully into encrypted SQLite repository.', 'error': False}, 201
        except aiosqlite.Error:
            return {'error': True ,'message': 'Conflict: Username/Email already exists or database file write block.'}, 400

    return async_to_sync(async_register)()


@user_management.route("/user/login", methods=["POST"])
def login_user():
    data = request.get_json() or {}
    useremail = data.get('username')
    password = data.get('password')

    async def async_login():
        async with get_db_connection() as conn:
            async with conn.execute(
                'SELECT username, hashed_password, permissions, tenant_name, password_changed, email FROM users WHERE email = ?', (useremail,)
            ) as cursor:
                row = await cursor.fetchone()

        if not row:
            return {'error': 'Unauthorized: Invalid email or password credentials supplied.'}, 401

        loop = asyncio.get_running_loop()
        is_valid_password = await loop.run_in_executor(None, lambda: bcrypt.checkpw(password.encode("utf-8"), row[1]))

        if not is_valid_password:
            return {'error': 'Unauthorized: Invalid email or password credentials supplied.'}, 401

        permissions_list = row[2].split(",") if row[2] else []
        username, tenant, pwd_state, email = row[0], row[3], row[4], row[5]       
        access_token = generate_user_jwt(row[0], permissions_list)

        return { 
            'access_token': access_token, 'tkn_typ': "Bearer", 'tenant': tenant, 'password_changed': pwd_state, 
            'permissions': permissions_list, 'email': email, 'username': username 
        }

    try:
        response_data = async_to_sync(async_login)()
        return response_data, 200
    except Exception as e:
        return jsonify({'error': f'Internal Server Error: {str(e)}'}), 500


@user_management.route("/user", methods=["GET"])
@require_permission('manage_users')
def list_system_users():
    try:
        all_users = async_to_sync(_get_all_users_async)()
        return jsonify({ 'status': 'success', 'total_users': len(all_users), 'users': all_users }), 200
        
    except Exception as e:
        return jsonify({ 'status': 'error', 'message': f'Failed to retrieve user index: {str(e)}' }), 500

asyncio.run(init_db())


@user_management.route("/user/role", methods=["POST"])
@require_permission('global_admin')
def add_role():
    data = request.get_json() or {}
    role_name, description = data.get('role_name'), data.get('description', '')

    if not role_name:
        return jsonify({'error': 'Bad Request: role_name parameter is mandatory.'}), 400

    async def run_async_block():
        await _save_role_async(role_name, description)

    try:
        async_to_sync(run_async_block)()
        return jsonify({'message': f"Role '{role_name}' saved successfully.", 'error': False}), 201
    except aiosqlite.Error:
        return jsonify({'error': True, 'message': 'Conflict: Role name already exists or database disk lock.'}), 400


@user_management.route("/permission", methods=["POST"])
@require_permission('global_admin')
def add_permission():
    data = request.get_json() or {}
    perm_name, description = data.get('perm_name'), data.get('description', '')

    if not perm_name:
        return jsonify({'error': 'Bad Request: perm_name parameter is mandatory.'}), 400
    async def run_async_block():
        await _save_permission_async(perm_name, description)

    try:
        async_to_sync(run_async_block)()
        return jsonify({'message': f"Permission '{perm_name}' saved successfully.", 'error': False}), 201
    except aiosqlite.Error:
        return jsonify({'error': True, 'message': 'Conflict: Permission signature already exists.'}), 400


@user_management.route("/user/permission", methods=["POST"])
@require_permission('manage_users')
def update_permissions():
    data = request.get_json() or {}
    permissions, email = data.get('permissions', []), data.get('email')

    async def async_update():
        try:
            async with get_db_connection() as conn:
                await conn.execute('UPDATE users SET permissions = ? WHERE email = ?', (permissions, email))
                await conn.commit()
            return {'message': 'User permissions updated successfully.', 'error': False}, 201
        except aiosqlite.Error as err:
            return {'error': True ,'message': f'Error while updated user permissions. {str(err)}'}, 400
    
    return async_to_sync(async_update)()


@user_management.route("/user/rbac/catalog", methods=["GET"])
@require_permission('manage_users')
def get_rbac_catalog():
    try:
        unified_catalog = async_to_sync(_get_unified_rbac_catalog_async)()
        return jsonify({ 'status': 'success', 'error': False, 'catalog': unified_catalog }), 200
    except Exception as e:
        return jsonify({'status': 'error', 'error': True, 'message': f'Failed to compile structural catalog: {str(e)}'}), 500
