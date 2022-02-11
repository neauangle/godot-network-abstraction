extends Node

signal debug(message)

"""
Provides:
	* parcelling such that each parcel is under 512 bytes
	* two ways to send:
		* send_data, which does not wait for a reply
		* wait for reply, such that you can send a message
		  and wait until you either get a reply or timeout
			* you must either
				wait for reply and call _udp_socket.fapi.get_info_for_completed_func(func_key)
				call abandon_send_data_wait_for_reply
		Both can be replies to received messages, and 
		these replies will be resent automatically on reception
		of packets with the same id as the one replied to.
		This also ensures no repeat packets get bubbled up from here.
		Note that packets received as a reply are not emitted
		in packet_received. It is expected that the handler
		is yielding on send_data_await_reply_completed.
	* compression - optional, set COMPRESSION_TYPE null to turn off
	* dictionary key minimisation
	* broadcasting and accumulating replies
	
"""
signal broadcast_data_accumulate_replies_completed()
signal send_data_await_reply_completed()
#not emitted for packets arrived in reply to waiting func keys
signal packet_received(data, sender_address, packet_id)
#emitted for any packet received- including those routed to be replies
signal any_packet_received(data, sender_address, packet_id)
#set null to disabled compression
#otherwise tries to only compress a packet if it would be worth it
var COMPRESSION_TYPE = File.COMPRESSION_ZSTD
#set null to never yield when sending parcels
const PARCELS_SENT_BEFORE_YIELD_UNTIL_IDLE = 4
#goal is < 512. Needs to save room for id, etc.
const NUM_BYTES_IN_PARCEL_BLOCK = 450
#windows doesnt like broadcasting on 255.255.255.255 (or android doesnt like receiving)
#const BROADCAST_ADDRESS = "255.255.255.255"
const BROADCAST_ADDRESS = "192.168.1.255"
var _fapi = preload('./FuncAwaitAPI.gd').new()

var _dummy_socket = PacketPeerUDP.new()
var _udp_socket = PacketPeerUDP.new()
var _key_generator = KeyGenerator.new()
#this is 0 if not listening, increments each time
#start_listening is called, and we only stop listening once it
#hits 0. This allows you to open it temporarily and then
#close it, but if someone else started us listening
#in the meatnime, we'll keep listening.
var _listening_num = 0
#works same as .istening_num
var _broadcast_enabled_num = 0
var _local_port
var _packet_timeout = 10
var _initial_resend_timer = 1
var _resend_timer_increment = 0.5
var _data_to_add_to_every_packet
#full id is client address string + packet id
var _full_ids_received_to_reply_sent = {}
#set in init
var _unminified_keys_to_minified_keys = {}
var _minified_keys_to_unminified_keys = {}
var _outgoing_parcel_info = {}
var _incoming_parcel_info = {}
var _wait_infos = {}
var _accumulator_wait_infos = {}

func set_data_to_add_to_every_packet(data_to_add_to_every_packet):
	_data_to_add_to_every_packet = data_to_add_to_every_packet
func set_packet_timeout_secs(packet_timeout):
	_packet_timeout = packet_timeout
func get_packet_timeout_secs():
	return _packet_timeout
func set_packet_resend_secs(first_resend, increment):
	_initial_resend_timer = first_resend
	_resend_timer_increment = increment

	
func set_minimisation_map(minimisation_map):
	_unminified_keys_to_minified_keys = minimisation_map
	for key in minimisation_map:
		_minified_keys_to_unminified_keys[minimisation_map[key]] = key


#############################################
#       FAPI
#############################################
func is_func_ongoing(func_key):
	return _fapi.is_func_ongoing(func_key)
func get_info_for_completed_func(func_key):
	return _fapi.get_info_for_completed_func(func_key)
func abandon_awaiting_func_completion(func_key):
	return _fapi.abandon_awaiting_func_completion(func_key)


func get_port():
	return _local_port

func is_listening():
	return _listening_num > 0


func enable_broadcast():
	_broadcast_enabled_num += 1
	if _broadcast_enabled_num == 1:
		#Network.emit_signal("debug", "Enabling broadcast")
		_udp_socket.set_broadcast_enabled(true)
		print('broadcastign enabled')

func disable_broadcast():
	_broadcast_enabled_num -= 1
	if _broadcast_enabled_num == 0:
		#Network.emit_signal("debug", "Disabling broadcast")
		_udp_socket.set_broadcast_enabled(false)
		print('broadcssting disabled')



#if already listeining, will not change port but will return OK
#each OKed listen should be paired with a stop_listening
func start_listening(local_port):
	var err = OK
	var port
	if not _udp_socket.is_listening():
		err = _udp_socket.listen(local_port)
		if err == OK:
			print('listening to %s' % local_port)
			_local_port = local_port
	if err == OK:
		_listening_num += 1

	return err


func stop_listening(resume_wait_funcs_as_timeout=true):
	_listening_num -= 1
	if _listening_num > 0:
		return
	for wait_info in _wait_infos.values().duplicate():
		if not resume_wait_funcs_as_timeout:
			wait_info['has-reply'] = true
			wait_info['reply-data'] = null
		_send_data_wait_for_reply_finished(wait_info)
	for wait_info in _accumulator_wait_infos.values().duplicate():
		_broadcast_data_accumulate_replies_finished(wait_info)
	if _udp_socket.is_listening():
		_udp_socket.close()
		print('stopped listening to %s' % _local_port)
	_outgoing_parcel_info.clear()
	_incoming_parcel_info.clear()
	_wait_infos.clear()
	_accumulator_wait_infos.clear()








func send_data(data, address, replying_to_id=null):
	var id = _key_generator.generate_key()
	while _outgoing_parcel_info.has(id):
		id = _key_generator.generate_key()
	if replying_to_id != null:
		_full_ids_received_to_reply_sent[str(address)+str(replying_to_id)] = data
	_parcel_send_packet(data, id, replying_to_id, address)





#resends  until it either times out or gets reply. eg:
#var func_key = _udp_socket.send_data_wait_for_reply(data, address)
#while _udp_socket.is_func_ongoing(func_key):
#	yield(_udp_socket, 'send_data_await_reply_completed')
#var func_result = _udp_socket.get_info_for_completed_func(func_key)
#func_result['timed-out']:
#		...handle_timeout
#else:
#	var reply_data = func_result['reply-data']
func send_data_wait_for_reply(data, address, replying_to_id=null, custom_resend_timer=1, custom_resend_timer_increment=0.5, custom_timeout=null):
	var id = _key_generator.generate_key()
	while _outgoing_parcel_info.has(id):
		id = _key_generator.generate_key()
	
	if replying_to_id != null:
		_full_ids_received_to_reply_sent[str(address)+str(replying_to_id)] = data
	
	var info = {
		'id': id,
		'data': data,
		'reply-id': null,
		'timed-out': false,
		'has-reply': false, 
		'reply-data': null, 
		'times-sent': 1, 
		'resend-timer': custom_resend_timer if custom_resend_timer != null else _initial_resend_timer, 
		'resend-timer-init': custom_resend_timer if custom_resend_timer != null else _initial_resend_timer, 
		'resend-timer-increment': custom_resend_timer_increment if custom_resend_timer_increment != null else _resend_timer_increment,
		'address': address,
		'timeout': custom_timeout if custom_timeout != null else _packet_timeout,
	}
	_wait_infos[id] = info
	
	var func_key = _fapi.get_add_key()
	info['func-key'] = func_key
	_parcel_send_packet(info['data'], info['id'], replying_to_id, info['address'])
	return func_key
	



#you must call enable_broadcast before calling this.
func broadcast_data_accumulate_replies(data, address_port, 
num_attempts, attempt_timeout, reattempt_even_if_a_reply_has_come):
	var id = _key_generator.generate_key()
	while _outgoing_parcel_info.has(id):
		id = _key_generator.generate_key()
	var info = {
		'id': id,
		'data': data,
		'reply-id-to-info': {},
		'times-to-attempt': num_attempts, 
		'attempt-timeout': attempt_timeout, 
		'attempt-timer': attempt_timeout,
		'reattempt-even-if-reply': reattempt_even_if_a_reply_has_come,
		'address': [BROADCAST_ADDRESS, address_port]
	}
	_accumulator_wait_infos[id] = info
	
	var func_key = _fapi.get_add_key()
	info['func-key'] = func_key
	_parcel_send_packet(info['data'], info['id'], null, info['address'])
	return func_key



func _broadcast_data_accumulate_replies_finished(info):
	var id = info['id']
	_accumulator_wait_infos.erase(id)
	
	var was_abandoned = _fapi.set_info_for_completed_func(info['func-key'], {
		'replies': info['reply-id-to-info'].values(),
	})
	if not was_abandoned:
		emit_signal('broadcast_data_accumulate_replies_completed')


func _send_data_wait_for_reply_finished(info):
	var id = info['id']
	_wait_infos.erase(id)
	
	var was_abandoned = _fapi.set_info_for_completed_func(info['func-key'], {
		'timed-out': not info['has-reply'], 
		'reply-data': info['reply-data'],
		'reply-id': info['reply-id'],
		'sent-id': info['id'],
		'address': info['address'] if info.has('address') else null
	})
	if not was_abandoned:
		emit_signal('send_data_await_reply_completed')

#for when you dont care about it really, but still want to 
#get the message through, 
func abandon_send_data_wait_for_reply(func_key, cancel_sending=true):
	if cancel_sending:
		for id in _wait_infos:
			if _wait_infos[id]['func-key'] == func_key:
				_wait_infos.erase(id)
				break
	_fapi.abandon_awaiting_func_completion(func_key)

















func _parcel_send_packet(data, id, reply_to_id, address):
	if data == null:
		print('Error: data sent through CustomUDPWrapper must be a dict')
		return
	
	if not _outgoing_parcel_info.has(id):
		if _data_to_add_to_every_packet != null:
			for key in _data_to_add_to_every_packet:
				data[key] = _data_to_add_to_every_packet[key]
		var minified_data = _get_minified(data)
		var bytes = var2bytes(minified_data)
		var should_compress = bytes.size() < NUM_BYTES_IN_PARCEL_BLOCK and COMPRESSION_TYPE != null
		var decompressed_size = bytes.size()
		if should_compress:
			bytes = bytes.compress(COMPRESSION_TYPE)
		_outgoing_parcel_info[id] = {}
		_outgoing_parcel_info[id]['parcels'] = []
		_outgoing_parcel_info[id]['ack-received'] = []
		var num_bytes = bytes.size()
		var num_parcels =  num_bytes / NUM_BYTES_IN_PARCEL_BLOCK
		if num_bytes % NUM_BYTES_IN_PARCEL_BLOCK != 0:
			num_parcels += 1
		for i in range(0, num_parcels):
			var sub_array = bytes.subarray(
				i*NUM_BYTES_IN_PARCEL_BLOCK,
				min(num_bytes,(i+1) * NUM_BYTES_IN_PARCEL_BLOCK) - 1
			)
			var parcel = {'i':i, 't':num_parcels, 'd': sub_array, 'id': id, 'rid': reply_to_id}
			if i == num_parcels - 1 and should_compress:
				parcel['s'] = decompressed_size
			parcel = var2bytes(parcel)

		
			_outgoing_parcel_info[id]['parcels'].push_back(parcel)
			_outgoing_parcel_info[id]['ack-received'].push_back(false)
		_outgoing_parcel_info[id]['sending'] = false
		_outgoing_parcel_info[id]['address'] = address
		_outgoing_parcel_info[id]['id'] = id
		_outgoing_parcel_info[id]['reply-to-id'] = reply_to_id
	_parcel_send_existing_packet(id, address)


func _parcel_send_existing_packet(id, address):
	if _outgoing_parcel_info[id]['sending']:
		return
	_outgoing_parcel_info[id]['sending'] = true
	var entry = _outgoing_parcel_info[id]
	var num_sent = 0
	#if entry['address'][0] == '127.0.0.1':
	#print(str(self) + '---> ' + entry['id'])
	for i in entry['parcels'].size():
		if _outgoing_parcel_info[id]['ack-received'][i]:
			continue
		num_sent += 1
		if PARCELS_SENT_BEFORE_YIELD_UNTIL_IDLE != null:
			if num_sent % PARCELS_SENT_BEFORE_YIELD_UNTIL_IDLE == 0:
				yield(get_tree(), 'idle_frame')
				if not is_inside_tree():
					return
				if not _outgoing_parcel_info.has(id):
					return
#		if randf() < 0.25:
#			print('fail')
#		else:
#			print('ok'
		
		_udp_socket.set_dest_address(entry['address'][0], entry['address'][1])
		#print(entry)
		#print(id)
		_udp_socket.put_packet(entry['parcels'][i])
	_outgoing_parcel_info[id]['sending'] = false


func _process_incoming_packet(bytes, address):
	var parcel = bytes2var(bytes)
	var id = parcel['id']
	#if address[0] == '127.0.0.1':
	#print(str(self)+'<--- ' + parcel['id'])
	#else:
		#print('here')
	
	if parcel.has('r'): #'r' is array of indexes already recieved by them
		if _incoming_parcel_info.has(id):
			var received = parcel['r']
			for index in received:
				_incoming_parcel_info[id]['ack-received'][index] = true
			_parcel_send_existing_packet(id, address)
		return
	var index = parcel['i']
	if not _incoming_parcel_info.has(id):
		var parcels = []
		for i in parcel['t']:#total number parcels being sent
			parcels.push_back(null)
		_incoming_parcel_info[id] = {
			'parcels': parcels,
			'secs-before-request-missing': 1.5,
			'address': address,
			'id': parcel['id'],
			'rid': parcel['rid'],
		}
	
	var parcel_info = _incoming_parcel_info[id]
	
	if index == parcel['t'] - 1:
		if parcel.has('s'):
			parcel_info['decompressed-size'] = parcel['s']
	
	if parcel_info.has('result'):
		return parcel_info['result']
	
	parcel_info['secs-before-request-missing'] = 1.5
	
	var parcels = parcel_info['parcels']
	parcels[index] = parcel
	for par in parcels:
		if par == null:
			return null
	var m = PoolByteArray()
	
	for par in parcels:
		for b in par['d']:
			m.push_back(b)
	if parcel_info.has('decompressed-size'):
		m = m.decompress(
		parcel_info['decompressed-size'], 
		COMPRESSION_TYPE
	)
	var data = bytes2var(m)
	if data == null:
		print('Error: data received by CustomUDPWrapper must be a dict')
		return
	data = _get_unminified(data)
	parcel_info['data'] = data
	parcel_info['result'] = {'data': data, 'id': id, 'rid': parcel_info['rid']}
	return parcel_info['result']


func _process(delta):
	for id in _incoming_parcel_info:
		var parcel_info = _incoming_parcel_info[id]
		parcel_info['secs-before-request-missing'] -= delta
		if parcel_info['secs-before-request-missing'] < 0:
			parcel_info['secs-before-request-missing'] = 4
			var address = parcel_info['address']
			
			
			var received = []
			for i in parcel_info['parcels'].size():
				if parcel_info['parcels']:
					received.push_back(i)
			if received.size() == parcel_info['parcels'].size():
				continue
			var data = {}
			data['r'] = received
			data['id'] = id
			#print(address)
			_udp_socket.set_dest_address(address[0], address[1])
			#print(data)
			_udp_socket.put_packet(var2bytes(data))
	
	while _udp_socket.get_available_packet_count() > 0:
		var bytes = _udp_socket.get_packet()
		var ip = _udp_socket.get_packet_ip()
		var port = _udp_socket.get_packet_port()
		var sender_address = [ip, port]
		var result = _process_incoming_packet(bytes, sender_address)
		if result == null:
			continue
		var data = result
		#print(data)
		var full_id = str(sender_address) + str(data['id'])
		if _full_ids_received_to_reply_sent.has(full_id):
			if _full_ids_received_to_reply_sent[full_id] != null:
				#print('resending reply')
				send_data(
					_full_ids_received_to_reply_sent[full_id],
					sender_address, data['id']
				)
			continue
		
		#_full_ids_received_to_reply_sent[full_id] = null
		
		emit_signal('any_packet_received', data['data'], sender_address, data['id'])
		
		var is_reply_to =  data['rid']
		if is_reply_to != null:
			#emit_signal("debug", 'received reply to %s' % is_reply_to)
			if _wait_infos.has(is_reply_to):
				var info = _wait_infos[is_reply_to]
				info['has-reply'] = true
				info['reply-data'] = data['data']
				info['reply-id'] = data['id']
				info['address'] = sender_address
				_send_data_wait_for_reply_finished(info)
			elif _accumulator_wait_infos.has(is_reply_to):
				var wait_info = _accumulator_wait_infos[is_reply_to]
				var reply_info = {
					'reply-id': data['id'],
					'reply-data': data['data'],
					'address': sender_address,
				}
				_accumulator_wait_infos[is_reply_to]['reply-id-to-info'][data['id']] = reply_info
				if not wait_info['reattempt-even-if-reply']:
					_broadcast_data_accumulate_replies_finished(wait_info)
					
		else:
			emit_signal('packet_received', data['data'], sender_address, data['id'])
	
	var timedout_waits = []
	for wait_info in _wait_infos.values():
		wait_info['timeout'] -= delta
		if wait_info['timeout'] < 0:
			timedout_waits.push_back(wait_info)
		else:
			wait_info['resend-timer'] -= delta
			if wait_info['resend-timer'] < 0:
				wait_info['resend-timer'] = wait_info['resend-timer-init'] + wait_info['times-sent'] * wait_info['resend-timer-increment']
				wait_info['times-sent'] += 1
				_parcel_send_existing_packet(wait_info['id'], wait_info['address'])
	for wait_info in timedout_waits:
		_send_data_wait_for_reply_finished(wait_info)
		
	var timedout_accumulator_waits = []
	for wait_info in _accumulator_wait_infos.values():
		wait_info['attempt-timer'] -= delta
		if wait_info['attempt-timer'] < 0:
			wait_info['times-to-attempt'] -= 1
			if wait_info['times-to-attempt'] == 0:
				timedout_accumulator_waits.push_back(wait_info)
			else:
				wait_info['attempt-timer'] = wait_info['attempt-timeout']
				_parcel_send_existing_packet(wait_info['id'], wait_info['address'])
	for wait_info in timedout_accumulator_waits:
		_broadcast_data_accumulate_replies_finished(wait_info)






func _get_minified(data):
	if not data:
		return data
	if _unminified_keys_to_minified_keys.empty():
		return data
	else:
		var new_data = {}
		for key in data:
			if _unminified_keys_to_minified_keys.has(key):
				new_data[_unminified_keys_to_minified_keys[key]] = data[key]
			else:
				new_data[key] = data[key]
		return new_data

func _get_unminified(data):
	if not data:
		return data
	if _unminified_keys_to_minified_keys.empty():
		return data
	else:
		var new_data = {}
		for key in data:
			if _minified_keys_to_unminified_keys.has(key):
				new_data[_minified_keys_to_unminified_keys[key]] = data[key]
			else:
				new_data[key] = data[key]
		return new_data



class KeyGenerator extends Reference:
	const MAX = 1000000000000
	var _unique_key_tracker = int(int(abs(rand_seed(OS.get_unix_time())[1])) % MAX)
	func generate_key():
		var key = _unique_key_tracker
		_unique_key_tracker += 1
		if _unique_key_tracker % 25 == 0 and randf() < 0.5:
			_unique_key_tracker += randi() % 500
		if _unique_key_tracker > MAX:
			_unique_key_tracker = 0
		return '%X' % key
