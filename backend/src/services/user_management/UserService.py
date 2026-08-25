import asyncio
import bcrypt
import aiosqlite
from contextlib import asynccontextmanager
import os
import functools
from flask import request
import jwt
from services.user_management.UserAccessLevel import UserAccessLevel
import logging

DB_ENCRYPTION_KEY = os.environ.get("SQLITE_DB_ENCRYPTION_KEY")
ROOT_USERNAME = os.environ.get("ROOT_USERNAME", "root")
ROOT_PASSWORD = os.environ.get("ROOT_PASSWORD", "rootpass")
NAMESPACE = os.environ.get('USER_NAMESPACE', "default")
DB_FILE = "e2e_data_users.db"

JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY")
JWT_ALGORITHM = "HS256"

def require_permission(perm: str|list):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            token = request.cookies.get('access_token')

            if not token:
                auth_header = request.headers.get("Authorization")
                token = auth_header.split(" ")[1] if auth_header != None else ''

            if ((token is None or token == '') and perm != 'regular:requester'):
                return {'message': 'Unauthorized: Missing or malformed Authorization header token.', 'error': True}, 401
            
            try:
                if(token in ['undefined',None,''] and (perm == 'regular:requester' or 'regular:requester' in perm)):
                    return f(*args, **kwargs)
                
                payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
                if type(perm) in [str, list]: 
                    required_permission = perm if type(perm) == list else [perm]
                
                allowed_perm = set(required_permission) & set(payload.get("permissions", []))

                if len(allowed_perm) < 1 and perm != 'regular:requester':
                    return {'message': f"Forbidden: Insufficient privileges. Missing '{' or '.join(required_permission)}'.", 'error': True}, 403
                
                [request.user_context, request.permissions, request.token] = [payload, payload.get("permissions", []), token]
                               
                return f(*args, **kwargs)
                
            except jwt.ExpiredSignatureError:
                return {'message': 'Unauthorized: Token session expired. Please log in again.', 'error': True}, 401
            except jwt.InvalidTokenError:
                return {'message': 'Unauthorized: Invalid token modification or invalid signature detected.', 'error': True}, 401
        return wrapper
    return decorator


@asynccontextmanager
async def get_db_connection():
    conn = await aiosqlite.connect(DB_FILE)
    try:
        await conn.execute(f"PRAGMA key = '{DB_ENCRYPTION_KEY}'")
        yield conn
    finally:
        await conn.close()


UserAccessLevel.db_connection = get_db_connection


class UserService:

    access_level: UserAccessLevel = UserAccessLevel

    async def get_all_users_async():
        async with get_db_connection() as conn:
            async with conn.execute('SELECT id, username, permissions, password_changed, tenant_name, expire_date, email, data_access_level FROM users') as cursor:
                rows, users = await cursor.fetchall(), []
            
            for r in rows: 
                users.append({ 
                    'usr': r[1], 'data_access_level': r[7],
                    'permissions': r[2].split(',') if r[2] else [], 'pwd_chgd': r[3], 
                    'tenant_name': r[4], 'pwd_exp': r[5], 'email': r[6] 
                })

            return users


    async def save_role_async(role_name: str, description: str):
        async with get_db_connection() as conn:
            await conn.execute("INSERT INTO roles (role_name, description) VALUES (?, ?)", (role_name, description))
            await conn.commit()
            await conn.execute('CHECKPOINT')


    async def _save_permission_async(perm_name: str, description: str):
        async with get_db_connection() as conn:
            await conn.execute("INSERT INTO permissions (perm_name, description) VALUES (?, ?)", (perm_name, description))
            await conn.commit()
            await conn.execute('CHECKPOINT')


    async def get_unified_rbac_catalog_async():
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


    async def seed_root_user():
        async with get_db_connection() as conn:
            async with conn.execute('SELECT id FROM users WHERE email = ?', (f'{ROOT_USERNAME}@{NAMESPACE}',)) as cursor:
                row = await cursor.fetchone()
            
            if not row:
                loop = asyncio.get_running_loop()
                salt = await loop.run_in_executor(None, bcrypt.gensalt)
                hashed_root_password = await loop.run_in_executor(None, lambda: bcrypt.hashpw(ROOT_PASSWORD.encode("utf-8"), salt))            
                root_permissions = 'global_admin,manage_users,query:dw'
                
                await conn.execute(
                    'INSERT INTO users (username, hashed_password, permissions, password_changed, tenant_name, email) VALUES (?, ?, ?, ?, ?, ?)',
                    (ROOT_USERNAME, hashed_root_password, root_permissions, 'No', NAMESPACE, f'{ROOT_USERNAME}@{NAMESPACE}')
                )
                await conn.commit()
                await conn.execute('CHECKPOINT')
                logging.error("[SEED] Root user seeded successfully.")
            else:
                logging.error("[SEED] Root user already exists. Skipping seed step.")


    async def seed_rbac():
        base_roles = [
            ('global_admin', 'Full administrative access'), 
            ('analyst', 'Have full access to the to the DW for deep analysis'),
            ('manage_users', 'Runs user management operation like create, add/revoke permission, deactivate and delete'),
        ]
        base_permissions = [
            ('create:pipeline', 'Ability to create new data pipeline'),
            ('view:pipeline', 'View previously created pipelines'),
            ('schedule:pipeline', 'Schedule pipeline that was previously created'),
            ('query:dw', 'Allow user to run query against the Data warehouse'),
        ]

        async with get_db_connection() as conn:
            for r_name, r_desc in base_roles:
                await conn.execute("INSERT OR IGNORE INTO roles (role_name, description) VALUES (?, ?)", (r_name, r_desc))
                
            for p_name, p_desc in base_permissions:
                await conn.execute("INSERT OR IGNORE INTO permissions (perm_name, description) VALUES (?, ?)", (p_name, p_desc))
                
            await conn.commit()


    async def init_db():
        async with get_db_connection() as conn:
            await conn.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    username TEXT UNIQUE NOT NULL, 
                    email TEXT UNIQUE NOT NULL,
                    hashed_password BLOB NOT NULL, 
                    permissions TEXT NOT NULL, 
                    password_changed TEXT, 
                    tenant_name TEXT, 
                    expire_date TEXT,
                    data_access_level TEXT NOT NULL DEFAULT '{}',                
                    CONSTRAINT check_valid_json CHECK (json_valid(data_access_level))
                );

                CREATE TABLE IF NOT EXISTS roles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    role_name TEXT UNIQUE NOT NULL, 
                    description TEXT,
                    data_access_level TEXT NOT NULL DEFAULT '{}',
                    permissions TEXT NOT NULL DEFAULT '{}',
                    CONSTRAINT check_valid_json CHECK (json_valid(data_access_level)),
                    CONSTRAINT check_valid_json CHECK (json_valid(permissions))
                );
                
                CREATE INDEX IF NOT EXISTS idx_users_security_lookup ON users(username);
                CREATE INDEX IF NOT EXISTS idx_users_security_lookup ON roles(permissions);
            """)
            await conn.execute("CREATE TABLE IF NOT EXISTS permissions (id INTEGER PRIMARY KEY AUTOINCREMENT, perm_name TEXT UNIQUE NOT NULL, description TEXT)")
            await conn.commit()

        await UserService.seed_rbac()
        await UserService.seed_root_user()

    
    async def register_user(username, hashed_pwd, permissions, email, tenant_name):
        async with get_db_connection() as conn:
            await conn.execute(
                'INSERT INTO users (username, hashed_password, permissions, email, tenant_name) VALUES (?, ?, ?, ?, ?)', (username, hashed_pwd, permissions, email, tenant_name)
            )
            await conn.commit()
            await conn.execute('CHECKPOINT')

    
    async def handle_login(useremail):
        row = None
        async with get_db_connection() as conn:
            async with conn.execute(
                """
                    SELECT username, hashed_password, permissions, tenant_name, password_changed, email FROM users WHERE email = ?
                """, (useremail,)
            ) as cursor:
                row = await cursor.fetchone()

        return row

    
    async def change_password(useremail, password):
        try:
            async with get_db_connection() as conn:
                await conn.execute(
                    "UPDATE users SET hashed_password = ?, password_changed = 'Yes' WHERE email = ?", (password, useremail)
                )
                await conn.commit()
                return True
        except Exception as err:
                return f'Error while changing the password: {str(err)}'


    async def update_permission(permissions, email):
        async with get_db_connection() as conn:
            await conn.execute('UPDATE users SET permissions = ? WHERE email = ?', (permissions, email))
            await conn.commit()

    
    async def update_tables_level_perm(role_name, tables_constraint):
        from services.user_management.UserAccessLevel import UserAccessLevel
        await UserAccessLevel.save_access_tables_constraint(get_db_connection(), role_name, tables_constraint)