from app.db import engine, Base
from app import models  # noqa: F401  (ensures models are imported/registered)

def main():
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully.")

if __name__ == "__main__":
    main()
