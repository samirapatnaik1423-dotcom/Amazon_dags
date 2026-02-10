from airflow.www.fab_security.manager import SecurityManager
from airflow.www.app import create_app

try:
    app = create_app()
    with app.app_context():
        security_manager = SecurityManager(app.appbuilder)
        role_admin = security_manager.find_role('Admin')
        
        if not role_admin:
            print('❌ Admin role not found!')
            exit(1)
        
        user = security_manager.find_user(username='admin')
        if user:
            print('Admin user exists, resetting password...')
            security_manager.update_user(user, password='admin')
        else:
            print('Creating new admin user...')
            security_manager.add_user(
                username='admin',
                first_name='Admin',
                last_name='User',
                email='admin@example.com',
                role=role_admin,
                password='admin'
            )
        print('✅ Success! Login at http://localhost:8080 with admin/admin')
except Exception as e:
    print(f'❌ Error: {e}')
    import traceback
    traceback.print_exc()
