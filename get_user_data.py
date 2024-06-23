import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firestore with your service account key
cred = credentials.Certificate('firebase_credentials.json')
firebase_admin.initialize_app(cred)

db = firestore.client()

def fetch_all_users_data():
    users_ref = db.collection('users')
    users_docs = users_ref.stream()
    
    all_users_data = []
    
    for user_doc in users_docs:
        user_data = user_doc.to_dict()
        user_data['email'] = user_doc.id  # Document ID is assumed to be the user's email
        # Ensure all expected fields are present, set defaults if not
        expected_fields = [
            'account_Type', 'addr_line', 'd_Period', 'dob', 'email', 'full_name',
            'gender', 'password', 'phoneNumber', 'profile_URL', 'uid', 'zipcd'
        ]
        for field in expected_fields:
            if field not in user_data:
                user_data[field] = None  # or set to a default value
        
        all_users_data.append(user_data)
    
    return all_users_data

# Retrieve all users' data
all_users_data = fetch_all_users_data()

# Print or process the retrieved data
for user_data in all_users_data:
    print(user_data)