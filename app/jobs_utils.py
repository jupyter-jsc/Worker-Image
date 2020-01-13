from app import unicore_utils, utils_file_loads, tunnel_communication, hub_communication, orchestrator_communication

def stop_job(app_logger, uuidcode, servername, system, request_headers, app_urls, send_cancel=True):
    app_logger.trace("{} - Create UNICORE Header".format(uuidcode))
    if ':' not in servername:
        servername = "{}:{}".format(request_headers.get('escapedusername'), servername)
    
    unicore_header, accesstoken, expire = unicore_utils.create_header(app_logger,
                                                                      uuidcode,
                                                                      request_headers,
                                                                      app_urls.get('hub', {}).get('url_proxy_route'),
                                                                      app_urls.get('hub', {}).get('url_token'),
                                                                      request_headers.get('escapedusername'),
                                                                      servername)
    if send_cancel:
        app_logger.debug("{} - Send cancel to JupyterHub".format(uuidcode))
        hub_communication.cancel(app_logger,
                                 uuidcode,
                                 app_urls.get('hub', {}).get('url_proxy_route'),
                                 app_urls.get('hub', {}).get('url_cancel'),
                                 request_headers.get('jhubtoken'),
                                 "JupyterLab named {} was stopped".format(servername),
                                 request_headers.get('escapedusername'),
                                 servername)

    # Get certificate path to communicate with UNICORE/X Server
    app_logger.trace("{} - FileLoad: UNICORE/X certificate path".format(uuidcode))
    unicorex = utils_file_loads.get_unicorex()
    cert = unicorex.get(system, {}).get('certificate', False)
    app_logger.trace("{} - FileLoad: UNICORE/X certificate path Result: {}".format(uuidcode, cert))

    # Get logs from the UNICORE workspace. Necessary for support
    app_logger.debug("{} - Copy_log".format(uuidcode))
    try:
        unicore_utils.copy_log(app_logger,
                               uuidcode,
                               unicore_header,
                               request_headers.get('filedir'),
                               request_headers.get('kernelurl'),
                               cert)
    except:
        app_logger.exception("{} - Could not copy log.".format(uuidcode))

    # Abort the Job via UNICORE
    app_logger.debug("{} - Abort Job".format(uuidcode))
    unicore_utils.abort_job(app_logger,
                            uuidcode,
                            request_headers.get('kernelurl'),
                            unicore_header,
                            cert)

    # Destroy the Job via UNICORE
    app_logger.debug("{} - Destroy Job".format(uuidcode))
    unicore_utils.destroy_job(app_logger,
                              uuidcode,
                              request_headers.get('kernelurl'),
                              unicore_header,
                              cert)

    # Kill the tunnel
    tunnel_info = { "servername": servername }
    try:
        app_logger.debug("{} - Close ssh tunnel".format(uuidcode))
        tunnel_communication.close(app_logger,
                                   uuidcode,
                                   app_urls.get('tunnel', {}).get('url_tunnel'),
                                   tunnel_info)
    except:
        app_logger.exception("{} - Could not stop tunnel. tunnel_info: {} {}".format(uuidcode, tunnel_info, app_urls.get('tunnel', {}).get('url_tunnel')))

    # Remove Database entry for J4J_Orchestrator
    app_logger.debug("{} - Call J4J_Orchestrator to remove entry {} from database".format(uuidcode, servername))
    orchestrator_communication.delete_database_entry(app_logger,
                                                     uuidcode,
                                                     app_urls.get('orchestrator', {}).get('url_database'),
                                                     servername)

    return accesstoken, expire, unicore_header.get('X-UNICORE-SecuritySession')
