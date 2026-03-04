# Deploy to Streamlit Cloud

## Step-by-Step Deployment

1. **Go to Streamlit Cloud:**
   - Visit: https://share.streamlit.io/
   - Sign in with GitHub

2. **Create New App:**
   - Click "New app" button
   - Select your repository: `karthiga33/Pricing-calculator-excel`
   - Branch: `main`
   - Main file path: `app2.py`

3. **Add Secret (IMPORTANT):**
   - Click "Advanced settings"
   - In the "Secrets" section, add:
   ```toml
   GROQ_API_KEY = "your_groq_api_key_here"
   ```
   - Replace `your_groq_api_key_here` with your actual Groq API key from https://console.groq.com

4. **Deploy:**
   - Click "Deploy!"
   - Wait 2-3 minutes for deployment

5. **Share the URL:**
   - Once deployed, you'll get a URL like: `https://your-app-name.streamlit.app`
   - Share this URL with your teammates
   - They can use it directly without cloning or installing anything!

## Notes

- The API key is stored securely in Streamlit Cloud secrets
- Your teammates only need the URL to access the app
- No installation or setup required for users
- The app will automatically update when you push to GitHub
