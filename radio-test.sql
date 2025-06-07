-- Open a connection to a websocket server
CALL radio_subscribe('ws://127.0.0.1:20400');

.mode line

-- Show all subscriptions.
select * from radio_subscriptions();

-- Show all messages received so far.
select * from radio_received_messages();

.mode duckbox

-- Block until a message is received for 10 seconds.
CALL radio_listen(true, interval '10 seconds');

-- Show all messages received so far.
.mode line
select * from radio_received_messages();

-- Show the messages
select message_id, message, receive_time from radio_received_messages()
order by receive_time;

-- Sleep 10 seconds to accumulate messages.
CALL radio_sleep(interval '1 seconds');

-- Show the message
select message_id, message, receive_time from radio_received_messages()
order by receive_time;

.mode line

-- Show all subscriptions.
select * from radio_subscriptions();

.mode duckbox
CALL radio_transmit_message('ws://127.0.0.1:20400', null, 'TEST MESSAGE'::blob, 10, interval '1 minute');


.mode line
SELECT * FROM radio_subscription_transmit_messages('ws://127.0.0.1:20400');

.mode line
SELECT * FROM radio_subscription_received_messages('ws://127.0.0.1:20400');


.mode line
SELECT * FROM radio_subscription_transmit_messages('ws://127.0.0.1:20400');

.mode duckbox
CALL radio_transmit_message('ws://127.0.0.1:20400', null, 'TEST MESSAGE2'::blob, 1, interval '10 seconds');

.mode line
SELECT * FROM radio_subscription_transmit_messages('ws://127.0.0.1:20400');


CALL radio_subscription_transmit_messages_delete_finished('ws://127.0.0.1:20400');

.mode line
SELECT * FROM radio_subscription_transmit_messages('ws://127.0.0.1:20400');


CALL radio_transmit_message('ws://127.0.0.1:20400', null, 'test message 3'::blob, 1, interval '10 seconds');


SELECT * FROM radio_subscription_transmit_messages('ws://127.0.0.1:20400');

CALL radio_subscription_transmit_message_delete('ws://127.0.0.1:20400', 1003::ubigint);

SELECT * FROM radio_subscription_transmit_messages('ws://127.0.0.1:20400');

.mode duckbox
SELECT * from radio_flush(interval '10 seconds');

CALL radio_unsubscribe('ws://127.0.0.1:20400');


CALL radio_subscribe('redis-tcp://127.0.0.1:6379?channel=radio_test&socket_timeout=100ms');

select * from radio_subscriptions();

.mode duckbox
SELECT * FROM radio_subscription_transmit_messages('redis-tcp://127.0.0.1:6379?channel=radio_test&socket_timeout=100ms');

-- Block until a message is received for 10 seconds.
CALL radio_listen(true, interval '10 seconds');

select * from radio_received_messages();

CALL radio_sleep(interval '4 seconds');

select * from radio_received_messages();

CALL radio_subscription_transmit_messages_delete_finished('redis-tcp://127.0.0.1:6379?channel=radio_test&socket_timeout=100ms');
