import oracledb
from walletcredentials import uname, pwd, cdir, wltloc, wltpwd, dsn

def get_cursor():
    try:
        print("Connecting to Database, Getting Cursor Instance......")
        # Establish connection with the Oracle database
        connection = oracledb.connect(user=uname, password=pwd, dsn=dsn,
                                      config_dir=cdir, wallet_location=wltloc,
                                      wallet_password=wltpwd)
        return connection.cursor()
    except oracledb.DatabaseError as e:
        error, = e.args
        print("Database Error ! Unable to connect to database")
        print("Oracle-Error-Code:", error.code)
        print("Oracle-Error-Message:", error.message)
        return None
    except Exception as e:
        print("An unexpected error occurred:", str(e))
        return None
