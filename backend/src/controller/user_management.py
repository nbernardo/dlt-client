from datetime import datetime, timedelta
import asyncio
from flask import Blueprint, request, jsonify, make_response
import bcrypt
import jwt
import aiosqlite
from asgiref.sync import async_to_sync
import threading
from concurrent.futures import ThreadPoolExecutor
from services.user_management.UserService import (
    UserService, JWT_SECRET_KEY, JWT_ALGORITHM, require_permission
)

executor = ThreadPoolExecutor(max_workers=5)

user_management = Blueprint('authentication', __name__)
asyncio.run(UserService.init_db())

def generate_user_jwt(username: str, permissions: list) -> str:
    payload = {'sub': username, 'permissions': permissions, 'exp': datetime.utcnow() + timedelta(hours=24), 'iat': datetime.utcnow()}
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


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
            await UserService.register_user(username, hashed_pwd, permissions, email)
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
        
        row = await UserService.handle_login(useremail)
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
            'permissions': permissions_list, 'email': email, 'username': username, 'login_active': True
        }

    try:
        response_data = async_to_sync(async_login)()
        [age, tkn] = [3600 * 24, response_data.get('access_token')]

        response = make_response(jsonify(response_data), 200)

        response.set_cookie(key='access_token', value=tkn, httponly=False, secure=False, samesite='Lax', max_age=age)
        response.set_cookie(key='logged_in', value='true', httponly=False, secure=False, samesite='Lax', max_age=age)

        return response
    except Exception as e:
        return jsonify({'error': f'Internal Server Error: {str(e)}'}), 500


@user_management.route("/user", methods=["GET"])
@require_permission('manage_users')
def list_system_users():
    try:

        async def fetch_data_async():
            return await UserService.get_all_users_async()

        def run_in_isolated_loop():
            return asyncio.run(fetch_data_async())

        future = executor.submit(run_in_isolated_loop)
        all_users = future.result()

        return jsonify({ 'status': 'success', 'total_users': len(all_users), 'users': all_users }), 200

    except Exception as e:
        return jsonify({
            'status': 'error', 
            'error': True, 
            'message': f'Failed to list system users: {str(e)}'
        }), 500


@user_management.route("/user/role", methods=["POST"])
@require_permission('global_admin')
def add_role():
    data = request.get_json() or {}
    role_name, description = data.get('role_name'), data.get('description', '')

    if not role_name:
        return jsonify({'error': 'Bad Request: role_name parameter is mandatory.'}), 400

    async def run_async_block():
        await UserService.save_role_async(role_name, description)

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
        await UserService._save_permission_async(perm_name, description)

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
            await UserService.update_permission(permissions, email)
            return {'message': 'User permissions updated successfully.', 'error': False}, 201
        except aiosqlite.Error as err:
            return {'error': True ,'message': f'Error while updated user permissions. {str(err)}'}, 400
    
    return async_to_sync(async_update)()


@user_management.route("/user/rbac/catalog", methods=["GET"])
@require_permission('manage_users')
def get_rbac_catalog():
    try:
        unified_catalog = async_to_sync(UserService.get_unified_rbac_catalog_async)()
        return jsonify({ 'status': 'success', 'error': False, 'catalog': unified_catalog }), 200
    except Exception as e:
        return jsonify({'status': 'error', 'error': True, 'message': f'Failed to compile structural catalog: {str(e)}'}), 500


@user_management.route("/user/rbac/table", methods=["POST"])
@require_permission('manage_users')
def update_rbac_tables():
    try:
        data = request.get_json() or {}
        role_name = data.get('roleName')
        tables_constraint = data.get('tablesConstraint', {})

        def run_async_db_task(role, constraints):
            asyncio.run(UserService.update_tables_level_perm(role, constraints))

        threading.Thread(target=run_async_db_task, args=(role_name, tables_constraint), daemon=True).start()
        return jsonify({ 'status': 'success', 'error': False }), 200
        
    except Exception as e:
        return jsonify({'status': 'error', 'error': True, 'message': f'Failed to update user permissions: {str(e)}'}), 500
    

@user_management.route("/role/<role_name>/<namespace>/<dw>", methods=["GET"])
@require_permission('global_admin')
def ge_access_level_by_table(role_name, namespace = None, dw = None):

    try:
        async def fetch_data_async():
            return await UserService.access_level.get_access_tables_constraints(role_name, dw)
        
        def run_in_isolated_loop():
            return asyncio.run(fetch_data_async())

        future = executor.submit(run_in_isolated_loop)
        access_levels = future.result()

        return access_levels

    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        print(f"DATABASE ERROR CRASH: {str(e)}")
        return jsonify({"error": str(e)}), 500