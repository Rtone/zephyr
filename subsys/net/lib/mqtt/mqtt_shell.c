/*
 * Copyright (c) 2023 Gaël PORTAY
 *               2023 Rtone
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include <zephyr/kernel.h>
#include <zephyr/init.h>
#include <zephyr/shell/shell.h>
#include <zephyr/net/socket.h>
#include <zephyr/net/mqtt.h>

#include <zephyr/sys/printk.h>
#include <zephyr/random/random.h>
#include <string.h>
#include <errno.h>

#define LOG_LEVEL CONFIG_MQTT_SHELL_LOG_LEVEL
#include <zephyr/logging/log.h>
LOG_MODULE_REGISTER(mqtt_shell);

/* Buffers for MQTT client. */
static uint8_t rx_buffer[CONFIG_NET_BUF_DATA_SIZE];
static uint8_t tx_buffer[CONFIG_NET_BUF_DATA_SIZE];

/* The MQTT client struct. */
static struct mqtt_client client_ctx;

/* MQTT Broker details. */
static struct sockaddr_storage broker;

static void evt_cb(struct mqtt_client *const client,
		   const struct mqtt_evt *evt)
{
	struct mqtt_puback_param puback;
	int len, rc;

	if (evt->type != MQTT_EVT_PUBLISH) {
		return;
	}

	len = evt->param.publish.message.payload.len;
	while (len) {
		uint8_t data[128];
		int l;

		l = mqtt_read_publish_payload(&client_ctx, data,
			    len >= sizeof(data) - 1 ?  sizeof(data) - 1 : len);
		if (l < 0 && l != -EAGAIN) {
			LOG_WRN("Failed to read publish payload (l: %i)", l);
			break;
		}

		data[l] = '\0';
		len -= l;
		printk("%s", data);
	}
	printk("\n");

	puback.message_id = evt->param.publish.message_id;
	rc = mqtt_publish_qos1_ack(&client_ctx, &puback);
	if (rc < 0) {
		LOG_WRN("Failed to publish qos1 acknowledgment (rc: %i)", rc);
	}
}

/* Socket Poll */
static struct zsock_pollfd fds[1];

static void entry_point(void *d1, void *d2, void *d3)
{
	ARG_UNUSED(d1);
	ARG_UNUSED(d2);
	ARG_UNUSED(d3);

	for (;;) {
		int rc;

		rc = zsock_poll(fds, 1, SYS_FOREVER_MS);
		if (rc < 0) {
			LOG_ERR("Failed to poll (errno: %i)", errno);
			break;
		}

		if ((fds[0].revents & ZSOCK_POLLIN) == 0) {
			LOG_WRN("Not an input event (revents: 0x%x)",
				fds[0].revents);
			continue;
		}

		rc = mqtt_input(&client_ctx);
		if (rc < 0) {
			LOG_ERR("Failed to read input packet (rc: %i)", rc);
			break;
		}
	}
}

static K_THREAD_STACK_DEFINE(mqtt_shell_stack, 1500);
static struct k_thread thread;

static int cmd_connect(const struct shell *shell, size_t argc, char *argv[])
{
	struct sockaddr_in *broker4 = (struct sockaddr_in *)&broker;
	const char *user = NULL, *passwd = NULL;
	static struct mqtt_utf8 password;
	static struct mqtt_utf8 username;
	const char *addr, *client_id;
	unsigned long port;
	int rc;

	if (argc < 4) {
		shell_print(shell, "Usage: mqtt connect ADDR PORT CLIENT_ID"
				   " [USER]"
				   " [PASSWORD]");
		shell_error(shell, "Too few arguments!");
		return -EINVAL;
	}

	addr = argv[1];
	port = strtoul(argv[2], NULL, 0);
	client_id = argv[3];

	if (argc > 4) {
		user = argv[4];
	}

	if (argc > 5) {
		passwd = argv[5];
	}

	broker4->sin_family = AF_INET;
	broker4->sin_port = htons(port);
	zsock_inet_pton(AF_INET, addr, &broker4->sin_addr);

	/* MQTT client configuration */
	client_ctx.broker = &broker;
	client_ctx.evt_cb = evt_cb;

	client_ctx.client_id.utf8 = (uint8_t *)client_id;
	client_ctx.client_id.size = strlen(client_id);

	if (passwd) {
		password.utf8 = (uint8_t *)passwd;
		password.size = strlen(passwd);

		client_ctx.password = &password;
	} else {
		client_ctx.password = NULL;
	}

	if (user) {
		username.utf8 = (uint8_t *)user;
		username.size = strlen(user);

		client_ctx.user_name = &username;
	} else {
		client_ctx.user_name = NULL;
	}

	client_ctx.protocol_version = MQTT_VERSION_3_1_1;

	/* MQTT buffers configuration */
	client_ctx.rx_buf = rx_buffer;
	client_ctx.rx_buf_size = sizeof(rx_buffer);
	client_ctx.tx_buf = tx_buffer;
	client_ctx.tx_buf_size = sizeof(tx_buffer);

	/* MQTT transport configuration */
	client_ctx.transport.type = MQTT_TRANSPORT_NON_SECURE;

	rc = mqtt_connect(&client_ctx);
	if (rc) {
		shell_error(shell, "Failed to connect (rc: %i)", rc);
		return rc;
	}

	fds[0].events = ZSOCK_POLLIN;

	rc = zsock_poll(fds, 1, 5000);
	if (rc < 0) {
		shell_error(shell, "Failed to poll input event (errno: %i)",
			    errno);
		rc = -errno;
		goto abort;
	}

	rc = mqtt_input(&client_ctx);
	if (rc < 0) {
		shell_error(shell, "Failed to read input packet (rc: %i)", rc);
		goto abort;
	}

	k_thread_create(&thread, mqtt_shell_stack,
			K_THREAD_STACK_SIZEOF(mqtt_shell_stack),
			entry_point,
			NULL,
			NULL,
			NULL,
			K_PRIO_COOP(2),
			0,
			K_NO_WAIT);
	return 0;

abort:
	mqtt_abort(&client_ctx);
	return rc;
}

static int cmd_publish(const struct shell *shell, size_t argc, char *argv[])
{
	enum mqtt_qos qos = MQTT_QOS_1_AT_LEAST_ONCE;
	struct mqtt_publish_param param;
	int rc;

	if (argc < 3) {
		shell_print(shell, "Usage: mqtt publish TOPIC PAYLOAD"
				   " [QOS]");
		shell_error(shell, "Too few arguments!");
		return -EINVAL;
	}

	if (argc > 3) {
		qos = strtoul(argv[3], NULL, 0);
	}

	param.message.topic.qos = qos;
	param.message.topic.topic.utf8 = argv[1];
	param.message.topic.topic.size = strlen(argv[1]);
	param.message.payload.data = argv[2];
	param.message.payload.len = strlen(argv[2]);
	param.message_id = sys_rand32_get();
	param.dup_flag = 0U;
	param.retain_flag = 0U;

	rc = mqtt_publish(&client_ctx, &param);
	if (rc < 0) {
		shell_error(shell, "Failed to publish (rc: %i)", rc);
		return rc;
	}

	return 0;
}

static int cmd_subscribe(const struct shell *shell, size_t argc, char *argv[])
{
	enum mqtt_qos qos = MQTT_QOS_1_AT_LEAST_ONCE;
	struct mqtt_subscription_list param;
	struct mqtt_topic topic;
	int rc;

	if (argc < 2) {
		shell_print(shell, "Usage: mqtt subscribe TOPIC [QOS]");
		shell_error(shell, "Too few arguments!");
		return -EINVAL;
	}

	if (argc > 2) {
		qos = strtoul(argv[2], NULL, 0);
	}

	topic.topic.utf8 = argv[1];
	topic.topic.size = strlen(argv[1]);
	topic.qos = qos;
	param.list = &topic;
	param.list_count = 1;
	param.message_id = sys_rand32_get();

	rc = mqtt_subscribe(&client_ctx, &param);
	if (rc < 0) {
		shell_error(shell, "Failed to subscribe (rc: %i)", rc);
		return rc;
	}

	return 0;
}

static int cmd_unsubscribe(const struct shell *shell, size_t argc,
			   char *argv[])
{
	enum mqtt_qos qos = MQTT_QOS_1_AT_LEAST_ONCE;
	struct mqtt_subscription_list param;
	struct mqtt_topic topic;
	int rc;

	if (argc < 2) {
		shell_print(shell, "Usage: mqtt unsubscribe TOPIC [QOS]");
		shell_error(shell, "Too few arguments!");
		return -EINVAL;
	}

	if (argc > 2) {
		qos = strtoul(argv[2], NULL, 0);
	}

	topic.topic.utf8 = argv[1];
	topic.topic.size = strlen(argv[1]);
	topic.qos = qos;
	param.list = &topic;
	param.list_count = 1;
	param.message_id = sys_rand32_get();

	rc = mqtt_unsubscribe(&client_ctx, &param);
	if (rc < 0) {
		shell_error(shell, "Failed to unsubscribe (rc: %i)", rc);
		return rc;
	}

	return 0;
}

static int cmd_ping(const struct shell *shell, size_t argc, char *argv[])
{
	int rc;
	ARG_UNUSED(argc);
	ARG_UNUSED(argv);

	rc = mqtt_ping(&client_ctx);
	if (rc < 0) {
		shell_error(shell, "Failed to ping (rc: %i)", rc);
		return rc;
	}

	return 0;
}

static int cmd_disconnect(const struct shell *shell, size_t argc, char *argv[])
{
	int rc;
	ARG_UNUSED(argc);
	ARG_UNUSED(argv);

	rc = mqtt_disconnect(&client_ctx);
	if (rc < 0) {
		shell_error(shell, "Failed to disconnect (rc: %i)", rc);
		return rc;
	}

	k_thread_abort(&thread);
	return 0;
}

static int cmd_abort(const struct shell *shell, size_t argc, char *argv[])
{
	int rc;
	ARG_UNUSED(argc);
	ARG_UNUSED(argv);

	rc = mqtt_abort(&client_ctx);
	if (rc < 0) {
		shell_error(shell, "Failed to abord (rc: %i)", rc);
		return rc;
	}

	k_thread_abort(&thread);
	return 0;
}

static int cmd_live(const struct shell *shell, size_t argc, char *argv[])
{
	int rc;
	ARG_UNUSED(argc);
	ARG_UNUSED(argv);

	rc = mqtt_live(&client_ctx);
	if (rc < 0) {
		shell_error(shell, "Failed to live (rc: %i)", rc);
		return rc;
	}

	return 0;
}

static int cmd_keepalive_time_left(const struct shell *shell, size_t argc,
				   char *argv[])
{
	int rc;
	ARG_UNUSED(argc);
	ARG_UNUSED(argv);

	rc = mqtt_keepalive_time_left(&client_ctx);
	if (rc < 0) {
		shell_error(shell, "Failed to keepalive time left (rc: %i)",
			    rc);
		return rc;
	}

	printk("Keep-alive time left: %ims\n", rc);

	return 0;
}

SHELL_STATIC_SUBCMD_SET_CREATE(mqtt_cmds,
	SHELL_CMD_ARG(connect, NULL, "Connect to server",
			  cmd_connect, 4, 2),
	SHELL_CMD_ARG(ping, NULL, "Send ping",
			  cmd_ping, 0, 0),
	SHELL_CMD_ARG(disconnect, NULL, "Disconnect from server",
			  cmd_disconnect, 0, 0),
	SHELL_CMD_ARG(publish, NULL, "Publish message on topic",
			  cmd_publish, 3, 1),
	SHELL_CMD_ARG(subscribe, NULL, "Subscribe to topic",
			  cmd_subscribe, 2, 1),
	SHELL_CMD_ARG(unsubscribe, NULL, "Unsubscribe from topic",
			  cmd_unsubscribe, 2, 1),
	SHELL_CMD_ARG(abort, NULL, "Abort connection",
			  cmd_abort, 0, 0),
	SHELL_CMD_ARG(live, NULL, "Keep connection alive",
			  cmd_live, 0, 0),
	SHELL_CMD_ARG(keepalive-time-left, NULL, "Keep alive ",
			  cmd_keepalive_time_left, 0, 0),
	SHELL_SUBCMD_SET_END
);

static int cmd_mqtt(const struct shell *shell, size_t argc, char **argv)
{
	shell_error(shell, "%s: unknown parameter: %s", argv[0], argv[1]);
	return -EINVAL;
}

SHELL_CMD_ARG_REGISTER(mqtt, &mqtt_cmds, "MQTT shell commands", cmd_mqtt, 1, 0);
