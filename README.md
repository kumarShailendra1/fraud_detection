# PyFlink Fraud Detection Pipeline

A real-time fraud detection system built with Apache Flink and Python that processes financial transaction streams and identifies suspicious patterns using multiple detection rules.

## 🚀 Features

- **Real-time Transaction Processing**: Processes financial transactions in a streaming fashion
- **Multiple Fraud Detection Rules**:
  - High Amount Fraud Detection (>$3000)
  - Suspicious Merchant Detection
  - International Location Fraud Detection
- **Realistic Transaction Generation**: Built-in transaction generator with configurable fraud patterns
- **Real-time Alerting**: Immediate fraud alerts with detailed information
- **Risk Scoring**: Dynamic risk assessment for each detected fraud pattern
- **Comprehensive Logging**: Detailed debugging and monitoring capabilities

## 📋 Requirements

### System Requirements
- Python 3.7+
- Java 8 or 11 (required for Apache Flink)
- At least 4GB RAM

### Python Dependencies
```bash
apache-flink>=1.17.0
```

## 🛠️ Installation

1. **Clone the repository** (or create the project directory):
```bash
mkdir fraud_detection
cd fraud_detection
```

2. **Create a virtual environment**:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install dependencies**:
```bash
pip install apache-flink
```

4. **Add the main script** (`main.py`) to your project directory.

## 🔧 Usage

### Basic Usage

Run the fraud detection pipeline with default settings:

```bash
python main.py
```

### Configuration

The pipeline can be configured by modifying the parameters in the `TransactionGenerator` class:

```python
generator = TransactionGenerator(
    num_users=100,           # Number of unique users to simulate
    fraud_probability=0.15   # Probability of generating fraudulent transactions
)
```

### Fraud Detection Thresholds

Adjust detection thresholds in the `create_fraud_detection_pipeline()` function:

```python
# High amount fraud threshold
high_amount_fraud = transaction_stream.filter(HighAmountFraudDetector(threshold=3000.0))

# Add more suspicious merchants
class SuspiciousMerchantDetector(FilterFunction):
    def __init__(self):
        self.suspicious_merchants = {'Unknown_Merchant', 'Suspicious_Store', 'Your_Custom_Merchant'}
```

## 🏗️ Architecture

### Pipeline Flow

```
Transaction Generation → Stream Processing → Fraud Detection → Alert Generation → Console Output
```

### Components

1. **Transaction Generator**: Creates realistic financial transaction data
2. **Stream Processor**: PyFlink DataStream processing engine
3. **Fraud Detectors**: Multiple filter functions for different fraud patterns
4. **Alert Mapper**: Converts detected fraud transactions to structured alerts
5. **Alert Printer**: Outputs formatted fraud alerts to console

### Data Models

#### Transaction
```python
{
    "transaction_id": "txn_1750253595490_5840",
    "user_id": "user_0024",
    "amount": 132.54,
    "merchant": "Starbucks",
    "category": "food",
    "timestamp": 1750253595490,
    "location": "Houston"
}
```

#### Fraud Alert
```python
{
    "alert_id": "alert_1750253595490_1234",
    "transaction_id": "txn_1750253595490_5840",
    "user_id": "user_0024",
    "fraud_type": "HIGH_AMOUNT_FRAUD",
    "risk_score": 95.0,
    "reason": "Transaction amount $8500.00 exceeds normal limits",
    "timestamp": 1750253595490,
    "original_transaction": {...}
}
```

## 🔍 Fraud Detection Rules

### 1. High Amount Fraud Detection
- **Trigger**: Transactions exceeding $3,000
- **Risk Score**: Based on transaction amount (higher = more suspicious)
- **Use Case**: Detect unusually large purchases that may indicate stolen cards

### 2. Suspicious Merchant Detection
- **Trigger**: Transactions from merchants in the suspicious list
- **Risk Score**: 85/100
- **Use Case**: Identify transactions from known problematic merchants

### 3. International Location Fraud
- **Trigger**: Transactions from international locations
- **Risk Score**: 80/100
- **Use Case**: Detect potentially unauthorized international usage

## 📊 Sample Output

```
🚀 Starting PyFlink Fraud Detection System
============================================================
📝 Generating 200 transactions...
📊 Generated 200 transactions
   - High amount (>$3000): 6
   - Suspicious patterns: 29
   - Expected fraud alerts: 35

📄 Sample Transaction 1:
   ID: txn_1750253595490_5840
   User: user_0024
   Amount: $132.54
   Merchant: Starbucks
   Location: Houston

🚨 FRAUD ALERT DETECTED! 🚨
==================================================
Alert ID: alert_1750253595490_1234
Fraud Type: HIGH_AMOUNT_FRAUD
Risk Score: 95.0/100
User ID: user_0042
Transaction ID: txn_1750253595490_5840
Amount: $8,500.00
Merchant: Apple Store
Location: New York
Reason: Transaction amount $8500.00 exceeds normal limits
Timestamp: 2025-06-18 14:30:00
==================================================
```

## 🐛 Troubleshooting

### Common Issues

1. **Import Errors**
   ```
   ImportError: cannot import name 'DeserializationSchema'
   ```
   **Solution**: Ensure you're using the correct PyFlink version and imports as shown in the code.

2. **Java Runtime Issues**
   ```
   py4j.protocol.Py4JJavaError
   ```
   **Solution**: Ensure Java 8 or 11 is installed and JAVA_HOME is set correctly.

3. **Memory Issues**
   ```
   OutOfMemoryError
   ```
   **Solution**: Reduce the number of transactions or increase JVM heap size.

### Debug Mode

Enable detailed debugging by modifying the logging in `FraudAlertMapper`:
```python
print(f"🔍 Processing transaction for {self.fraud_type}: {transaction_json}")
```

## 🚧 Current Limitations

1. **Stateless Processing**: Current implementation doesn't maintain user state across transactions
2. **Simple Rules**: Fraud detection rules are basic and rule-based
3. **No Persistence**: Alerts are only printed to console
4. **Single Machine**: Runs on single machine only

## 🔮 Future Enhancements

### Planned Features
- [ ] **Velocity Fraud Detection**: Detect multiple transactions in short time windows
- [ ] **Machine Learning Integration**: Add ML-based fraud scoring
- [ ] **External Integrations**: 
  - Kafka for data ingestion
  - Database for alert storage
  - REST API for alert management
- [ ] **Advanced Analytics**: User behavior profiling and anomaly detection
- [ ] **Web Dashboard**: Real-time monitoring interface
- [ ] **Alert Routing**: Email, SMS, and webhook notifications

### Technical Improvements
- [ ] **State Management**: Implement proper stateful processing
- [ ] **Configuration Management**: External configuration files
- [ ] **Performance Optimization**: Parallel processing and tuning
- [ ] **Testing**: Comprehensive unit and integration tests

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Flink community for the excellent streaming platform
- PyFlink team for Python integration
- Financial industry best practices for fraud detection patterns

## 📞 Support

For questions or issues:
1. Check the troubleshooting section above
2. Review PyFlink documentation: https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/python/
3. Open an issue in the repository

---

**Built with ❤️ using Apache Flink and Python**