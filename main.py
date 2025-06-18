"""
PyFlink Fraud Detection Pipeline - Simplified Working Version
Real-time fraud detection system using PyFlink with transaction data streaming
"""

import json
import time
import random
from datetime import datetime, timedelta
from typing import Iterator, Dict, Any
from dataclasses import dataclass, asdict

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types
from pyflink.datastream.functions import MapFunction, FilterFunction


@dataclass
class Transaction:
    """Transaction data model"""
    transaction_id: str
    user_id: str
    amount: float
    merchant: str
    category: str
    timestamp: int
    location: str

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)


@dataclass
class FraudAlert:
    """Fraud alert data model"""
    alert_id: str
    transaction_id: str
    user_id: str
    fraud_type: str
    risk_score: float
    reason: str
    timestamp: int
    original_transaction: dict

    def to_dict(self):
        return asdict(self)


class TransactionGenerator:
    """Simplified transaction generator for streaming"""

    def __init__(self, num_users: int = 100, fraud_probability: float = 0.15):
        self.num_users = num_users
        self.fraud_probability = fraud_probability
        self.users = [f'user_{i:04d}' for i in range(1, num_users + 1)]

        self.merchants = {
            'groceries': ['Walmart', 'Target', 'Kroger', 'Safeway'],
            'electronics': ['BestBuy', 'Apple Store', 'Microsoft Store'],
            'gas': ['Shell', 'Exxon', 'Chevron', 'BP'],
            'food': ['McDonalds', 'Starbucks', 'Subway', 'Chipotle'],
            'travel': ['United Airlines', 'Hilton', 'Uber', 'Airbnb'],
            'other': ['Amazon', 'Unknown_Merchant']
        }

        self.locations = ['New York', 'Los Angeles', 'Chicago', 'Houston',
                         'Phoenix', 'Philadelphia', 'International']

        # User profiles for realistic patterns
        self.user_profiles = {}
        for user in self.users:
            self.user_profiles[user] = {
                'avg_amount': random.uniform(20, 200),
                'primary_location': random.choice(self.locations[:-1]),  # Exclude International
                'favorite_categories': random.sample(list(self.merchants.keys()), 3)
            }

    def generate_transaction(self) -> Transaction:
        """Generate a single transaction"""
        user_id = random.choice(self.users)
        profile = self.user_profiles[user_id]

        # Determine if this should be fraudulent
        is_fraud = random.random() < self.fraud_probability

        if is_fraud:
            fraud_type = random.choice(['high_amount', 'unusual_location', 'unusual_merchant'])

            if fraud_type == 'high_amount':
                amount = round(random.uniform(5000, 15000), 2)
                category = random.choice(list(self.merchants.keys()))
                merchant = random.choice(self.merchants[category])
                location = profile['primary_location']

            elif fraud_type == 'unusual_location':
                amount = round(random.uniform(100, 500), 2)
                category = random.choice(profile['favorite_categories'])
                merchant = random.choice(self.merchants[category])
                location = 'International'  # Suspicious location

            else:  # unusual_merchant
                amount = round(random.uniform(200, 1000), 2)
                category = 'other'
                merchant = 'Unknown_Merchant'
                location = profile['primary_location']
        else:
            # Normal transaction
            amount = round(random.uniform(10, profile['avg_amount'] * 1.5), 2)
            category = random.choice(profile['favorite_categories'])
            merchant = random.choice(self.merchants[category])
            location = profile['primary_location'] if random.random() < 0.9 else random.choice(self.locations[:-1])

        return Transaction(
            transaction_id=f"txn_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            user_id=user_id,
            amount=amount,
            merchant=merchant,
            category=category,
            timestamp=int(time.time() * 1000),
            location=location
        )


def create_transaction_stream(num_transactions=1000):
    """Generate a list of transactions for streaming"""
    generator = TransactionGenerator()
    transactions = []

    print(f"📝 Generating {num_transactions} transactions...")

    fraud_count = 0
    high_amount_count = 0

    for i in range(num_transactions):
        transaction = generator.generate_transaction()
        # Add some time progression
        transaction.timestamp = int(time.time() * 1000) + (i * 1000)  # 1 second intervals

        # Count fraud patterns for stats
        if transaction.amount > 3000:
            high_amount_count += 1
        if transaction.merchant == 'Unknown_Merchant' or transaction.location == 'International':
            fraud_count += 1

        transactions.append(json.dumps(transaction.to_dict()))

    print(f"📊 Generated {num_transactions} transactions")
    print(f"   - High amount (>$3000): {high_amount_count}")
    print(f"   - Suspicious patterns: {fraud_count}")
    print(f"   - Expected fraud alerts: {fraud_count + high_amount_count}")
    print()

    return transactions


class HighAmountFraudDetector(FilterFunction):
    """Detect transactions with unusually high amounts"""

    def __init__(self, threshold: float = 5000.0):
        self.threshold = threshold

    def filter(self, transaction_json: str) -> bool:
        try:
            transaction_data = json.loads(transaction_json)
            return transaction_data['amount'] > self.threshold
        except Exception as e:
            print(f"Error in HighAmountFraudDetector: {e}")
            return False


class SuspiciousMerchantDetector(FilterFunction):
    """Detect transactions from suspicious merchants"""

    def __init__(self):
        self.suspicious_merchants = {'Unknown_Merchant', 'Suspicious_Store'}

    def filter(self, transaction_json: str) -> bool:
        try:
            transaction_data = json.loads(transaction_json)
            return transaction_data['merchant'] in self.suspicious_merchants
        except Exception as e:
            print(f"Error in SuspiciousMerchantDetector: {e}")
            return False


class LocationFraudDetector(FilterFunction):
    """Detect transactions from unusual locations"""

    def filter(self, transaction_json: str) -> bool:
        try:
            transaction_data = json.loads(transaction_json)
            return transaction_data['location'] == 'International'
        except Exception as e:
            print(f"Error in LocationFraudDetector: {e}")
            return False


class FraudAlertMapper(MapFunction):
    """Convert detected fraud transactions to fraud alerts"""

    def __init__(self, fraud_type: str, base_risk_score: float = 75.0):
        self.fraud_type = fraud_type
        self.base_risk_score = base_risk_score

    def map(self, transaction_json: str) -> str:
        try:
            # Debug: Print what we're processing
            print(f"🔍 Processing transaction for {self.fraud_type}: {transaction_json[:100]}...")

            transaction_data = json.loads(transaction_json)

            # Validate transaction data has required fields
            required_fields = ['transaction_id', 'user_id', 'amount', 'merchant', 'location', 'timestamp']
            missing_fields = [field for field in required_fields if field not in transaction_data]
            if missing_fields:
                print(f"❌ Transaction missing fields: {missing_fields}")
                return json.dumps({
                    "error": f"Missing fields: {missing_fields}",
                    "original_data": transaction_json[:200]
                })

            # Calculate risk score based on fraud type and amount
            risk_score = self.base_risk_score
            if self.fraud_type == "HIGH_AMOUNT_FRAUD":
                risk_score = min(100.0, (transaction_data['amount'] / 1000) * 10)
            elif self.fraud_type == "LOCATION_FRAUD":
                risk_score = 80.0
            elif self.fraud_type == "MERCHANT_FRAUD":
                risk_score = 85.0

            # Determine reason
            reason_map = {
                "HIGH_AMOUNT_FRAUD": f"Transaction amount ${transaction_data['amount']:.2f} exceeds normal limits",
                "LOCATION_FRAUD": f"Transaction from suspicious location: {transaction_data['location']}",
                "MERCHANT_FRAUD": f"Transaction with suspicious merchant: {transaction_data['merchant']}"
            }

            # Create fraud alert manually to avoid dataclass issues
            fraud_alert_dict = {
                "alert_id": f"alert_{transaction_data['timestamp']}_{random.randint(1000, 9999)}",
                "transaction_id": transaction_data['transaction_id'],
                "user_id": transaction_data['user_id'],
                "fraud_type": self.fraud_type,
                "risk_score": risk_score,
                "reason": reason_map.get(self.fraud_type, "Suspicious transaction detected"),
                "timestamp": transaction_data['timestamp'],
                "original_transaction": transaction_data
            }

            result = json.dumps(fraud_alert_dict)
            print(f"✅ Created fraud alert: {fraud_alert_dict['alert_id']}")
            return result

        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error in FraudAlertMapper: {e}")
            print(f"   Raw data: {transaction_json[:200]}")
            return json.dumps({
                "error": f"JSON decode error: {str(e)}",
                "original_data": transaction_json[:200]
            })
        except Exception as e:
            print(f"❌ Unexpected error in FraudAlertMapper: {e}")
            print(f"   Transaction data: {transaction_json[:200]}")
            import traceback
            traceback.print_exc()
            return json.dumps({
                "error": f"Unexpected error: {str(e)}",
                "original_data": transaction_json[:200]
            })


class FraudAlertPrinter(MapFunction):
    """Print fraud alerts to console"""

    def map(self, fraud_alert_json: str) -> str:
        try:
            alert_data = json.loads(fraud_alert_json)

            # Check if this is an error alert
            if "error" in alert_data:
                print(f"⚠️  Error processing transaction: {alert_data.get('error')}")
                return fraud_alert_json

            # Validate required fields
            required_fields = ['alert_id', 'fraud_type', 'risk_score', 'user_id',
                             'transaction_id', 'reason', 'timestamp', 'original_transaction']

            missing_fields = [field for field in required_fields if field not in alert_data]
            if missing_fields:
                print(f"⚠️  Alert missing fields: {missing_fields}")
                return fraud_alert_json

            print("🚨 FRAUD ALERT DETECTED! 🚨")
            print("=" * 50)
            print(f"Alert ID: {alert_data['alert_id']}")
            print(f"Fraud Type: {alert_data['fraud_type']}")
            print(f"Risk Score: {alert_data['risk_score']:.1f}/100")
            print(f"User ID: {alert_data['user_id']}")
            print(f"Transaction ID: {alert_data['transaction_id']}")
            print(f"Amount: ${alert_data['original_transaction']['amount']:.2f}")
            print(f"Merchant: {alert_data['original_transaction']['merchant']}")
            print(f"Location: {alert_data['original_transaction']['location']}")
            print(f"Reason: {alert_data['reason']}")
            print(f"Timestamp: {datetime.fromtimestamp(alert_data['timestamp']/1000)}")
            print("=" * 50)
            print()

            return fraud_alert_json

        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in fraud alert: {e}")
            print(f"Raw data: {fraud_alert_json[:200]}...")
            return fraud_alert_json
        except Exception as e:
            print(f"❌ Error printing fraud alert: {e}")
            return fraud_alert_json


def create_fraud_detection_pipeline():
    """Create the main fraud detection pipeline"""

    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Set parallelism for demo

    print("🔄 Starting Fraud Detection Pipeline...")
    print("📊 Generating transaction stream...")

    # Generate transaction data
    transaction_data = create_transaction_stream(num_transactions=200)  # Reduced for testing

    # Create transaction stream from collection
    transaction_stream = env.from_collection(transaction_data)

    # Print a sample of the raw transactions for debugging
    class TransactionSamplePrinter(MapFunction):
        def __init__(self):
            self.count = 0

        def map(self, transaction_json: str) -> str:
            self.count += 1
            if self.count <= 3:  # Print first 3 transactions
                try:
                    tx_data = json.loads(transaction_json)
                    print(f"📄 Sample Transaction {self.count}:")
                    print(f"   ID: {tx_data['transaction_id']}")
                    print(f"   User: {tx_data['user_id']}")
                    print(f"   Amount: ${tx_data['amount']:.2f}")
                    print(f"   Merchant: {tx_data['merchant']}")
                    print(f"   Location: {tx_data['location']}")
                    print()
                except:
                    print(f"   Error parsing sample transaction: {transaction_json[:100]}")
            return transaction_json

    # Add sample printer to see raw data
    transaction_stream = transaction_stream.map(TransactionSamplePrinter())

    # Branch the stream for different fraud detection rules

    # 1. High Amount Fraud Detection
    high_amount_fraud = transaction_stream \
        .filter(HighAmountFraudDetector(threshold=3000.0)) \
        .map(FraudAlertMapper("HIGH_AMOUNT_FRAUD", 90.0)) \
        .map(FraudAlertPrinter())

    # 2. Suspicious Merchant Fraud Detection
    merchant_fraud = transaction_stream \
        .filter(SuspiciousMerchantDetector()) \
        .map(FraudAlertMapper("MERCHANT_FRAUD", 85.0)) \
        .map(FraudAlertPrinter())

    # 3. Location Fraud Detection
    location_fraud = transaction_stream \
        .filter(LocationFraudDetector()) \
        .map(FraudAlertMapper("LOCATION_FRAUD", 80.0)) \
        .map(FraudAlertPrinter())

    # Union all fraud streams for centralized processing
    all_fraud_alerts = high_amount_fraud.union(merchant_fraud).union(location_fraud)

    return env


def run_fraud_detection_demo():
    """Run the fraud detection demo"""

    print("🚀 Starting PyFlink Fraud Detection System")
    print("=" * 60)
    print("This demo will:")
    print("1. Generate 200 realistic transaction records")
    print("2. Apply multiple fraud detection rules:")
    print("   - High amount transactions (>$3000)")
    print("   - Suspicious merchants")
    print("   - International transactions")
    print("3. Print fraud alerts as they are detected")
    print("=" * 60)
    print()

    try:
        # Create and execute pipeline
        env = create_fraud_detection_pipeline()

        # Execute the pipeline
        print("▶️  Executing fraud detection pipeline...")
        print("   Processing transactions for fraud patterns...")
        print()
        env.execute("Fraud Detection Pipeline")

        print("\n✅ Fraud detection pipeline completed successfully!")

    except KeyboardInterrupt:
        print("\n🛑 Pipeline stopped by user")
    except Exception as e:
        print(f"❌ Pipeline error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run_fraud_detection_demo()