select radio_tune_in('http://example.com/stream');

select radio_tune_in('http://example.com/stream2');

select radio_tune_out('http://example.com/stream');


select * from radio_subscriptions();

call radio_subscription_add_message('http://example.com/stream2',
'message',
'Test message 1'::blob);

call radio_subscription_add_message('http://example.com/stream2',
'message',
'Test message 2'::blob);

call radio_subscription_add_message('http://example.com/stream2',
'error',
'Test error 1'::blob);

select * from radio_subscriptions();

select * from radio_subscription_messages('http://example.com/stream2', 'message');


select * from radio_subscription_messages('http://example.com/stream2', 'message');


select * from radio_messages();