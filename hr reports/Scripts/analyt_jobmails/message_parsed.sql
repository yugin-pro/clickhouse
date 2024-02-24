--message_parsed



select 
id,PlainBody,Subject,message_from 
,empty(ReplyTo) notReplied
,extract(message_from, '@([^\> ]+)') mail_domain
,extract(message_from, '(.*)\<') mail_company
,extract(Subject, 'was\ sent\ to\ (.*)') easyapply_company
, match(PlainBody,'not.+to.+proceed') declined_1
, match(PlainBody,'not.+to.+proceed') invaiting_1
from extracted.jobmails_messages

