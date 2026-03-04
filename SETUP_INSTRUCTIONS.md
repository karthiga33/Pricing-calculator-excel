# Setup Instructions for Teammates

## Quick Start

1. **Clone the repository:**
   ```bash
   git clone https://github.com/karthiga33/Pricing-calculator-excel.git
   cd Pricing-calculator-excel
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Setup API Key:**
   - Copy `.env.example` to `.env`:
     ```bash
     copy .env.example .env
     ```
   - Get your FREE Groq API key from: https://console.groq.com
   - Open `.env` file and replace `your_groq_api_key_here` with your actual API key:
     ```
     GROQ_API_KEY=gsk_your_actual_key_here
     ```

4. **Run the application:**
   ```bash
   streamlit run app2.py
   ```

## Notes

- The `.env` file is ignored by Git (won't be pushed)
- Each person needs their own free API key from https://console.groq.com
- No AWS credentials needed for app2.py (uses Groq instead of Bedrock)
- For AWS Bedrock version, use `app.py` instead (requires AWS credentials)
