# Airflow Setup

## Switch google creds

```bash
echo 'export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.gc/google_credentials_nsw_prop.json"' >> ~/.zshrc

source ~/.zshrc

echo $GOOGLE_APPLICATION_CREDENTIALS
```

You should now see your new google credentials being activated. 
