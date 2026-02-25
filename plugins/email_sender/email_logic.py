from airflow.operators.email import EmailOperator

class SMTPConnector:
    def send_email(self, subject, body, send_to, files_path=None):
        # params = {
        #     "task_id": "email_task",
        #     "conn_id": "smtp_connection",
        #     "from_email": "soham.rane@avenues.info",
        #     "to": send_to,
        #     "subject": subject,
        #     "html_content": body
        # }

        # if files_path:
        #     params["files"] = []
        #     for file in files_path:
        #         params["files"].append(file)

        # email_operator = EmailOperator(**params)
        # return email_operator.execute(context={})


        result = EmailOperator(
            task_id = "emaiL_task",
            conn_id = "smtp_connection",
            from_email = "soham.rane@avenues.info",
            to = "sharonmavelil03@gmail.com",
            subject = subject,
            html_content = body,
            files = file_path
        ).execute(context={})

        return result








