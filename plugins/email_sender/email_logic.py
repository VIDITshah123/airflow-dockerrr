from airflow.operators.email import EmailOperator

class SMTPConnector:
    def send_email(self, subject, body, send_to):
        email_operator = EmailOperator(
        task_id="email_task",
        conn_id="smtp_connection", 
        from_email='soham.rane@avenues.info', 
        to=send_to,
        subject=subject,
        html_content=body
        )
        return email_operator.execute(context={})






