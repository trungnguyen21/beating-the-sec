"""
Load users config via json file on disk
"""

import json
import traceback

from models.users import User, Company
from utils.logger import get_logger

logger = get_logger(__name__)

class UserConfig:
    def __init__(self):
        self.companies = {}
        self.users = []

    def get_users(self):
        return self.users

    def get_companies(self):
        return self.companies
    
    def load_configs(self, company_file_path: str, user_file_path: str):
        self.load_companies(company_file_path)
        self.load_user(user_file_path)
        return

    def load_companies(self, file_path: str):
        try:
            with open(file_path, "r") as f:
                companies = json.loads(f.read())
                for company in companies:
                    temp = Company.model_validate(company)
                    self.companies[temp.ticker] = temp
                logger.info(f"Loaded {len(companies)} companies successfully")
        except Exception as e:
            logger.info(f"Failed to parse company list: {str(e)}")
            logger.debug(traceback.format_exc())
            raise

    def load_user(self, file_path: str):
        try:
            with open(file_path, "r") as f:
                users = json.loads(f.read())
                for user in users:
                    watchlist = []
                    for ticker in user['watchlist']:
                        if ticker not in self.companies:
                            raise ValueError(f"Ticker '{ticker}' in users.json not found in company registry")
                        watchlist.append(self.companies[ticker])

                    user_obj = User(
                        id=user["id"],
                        name=user["name"],
                        watchlist=watchlist,
                        webhook=user["webhook"],
                    )
                    self.users.append(user_obj)
                logger.info(f"Loaded {len(users)} users successfully")
        except Exception as e:
            logger.info("Failed to load user list!!")
            logger.debug(traceback.format_exc())
            raise