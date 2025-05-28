CALL radio_subscribe('http://example.com/stream', 10::uinteger);

CALL radio_subscribe('http://example.com/stream2', 10::uinteger);

CALL radio_unsubscribe('http://example.com/stream');


select * from radio_subscriptions();

call radio_subscription_received_message_add('http://example.com/stream2',
'message',
'Test message 1'::blob);

call radio_subscription_received_message_add('http://example.com/stream2',
'message',
'Test message 2'::blob);

call radio_subscription_received_message_add('http://example.com/stream2',
'error',
'Test error 1'::blob);

select * from radio_subscriptions();

select * from radio_subscription_received_messages('http://example.com/stream2', 'message');


select * from radio_subscription_received_messages('http://example.com/stream2', 'message');

select * from radio_received_messages();

select * from radio_subscription_transmit_messages('http://example.com/stream2');

CALL radio_subscription_transmit_message_add('http://example.com/stream2', 'test message'::blob, 10::uinteger, interval '1 minute');

select * from radio_subscription_transmit_messages('http://example.com/stream2');
