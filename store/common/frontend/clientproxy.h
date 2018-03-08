// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/frontend/clientproxy.h:
 *   Interface for a client side proxy that issues transactions to
 *   the server side.
 *
 **********************************************************************/

#ifndef __CLIENTPROXY_H__
#define __CLIENTPROXY_H__

#include "store/common/transaction.h"
#include "store/common/promise.h"
#include <vector>

class ClientProxy
{
public:
    ClientProxy() {};
    virtual ~ClientProxy() {};

    virtual void Invoke(std::vector<rid_t> &rids,
			std::vector<op_t> &ops,
			bool ro,
			std::vector<std::string> &results) = 0;
};

#endif /* __CLIENTPROXY_H__ */
