#include <iostream>
#include <condition_variable>
#include <thread>
#include "crow.h"
#include "nlohmann/json.hpp"
#include <queue>

#define DISPATCH_PERIOD 1000
#define NUM_SERVERS 5
using json = nlohmann::json;

class MutexQueue
{
private:
    std::queue<json> queue;
    std::mutex &mtx;
    int brokerID;

public:
    MutexQueue(int bId, std::mutex &m) : mtx(m)
    {
        brokerID = bId;
    }
    void push(json value)
    {
        std::lock_guard<std::mutex> lock(mtx); // Locks the mutex before modifying the queue
        queue.push(value);
    }

    bool pop(json &value)
    {
        if (queue.empty())
        {
            return false;
        }
        value = queue.front();
        queue.pop();
        return true;
    }
    void createPackage(json &package)
    {
        std::lock_guard<std::mutex> lock(mtx);
        package = nlohmann::json::array();
        if (!queue.empty())
        {
            json transaction;
            while (pop(transaction))
            {
                transaction["brokerID"] = brokerID;
                package.push_back(transaction);
            }
        }
    }
};

// Function to verify JSON structure
// The valid structure is:
// {
//  "key": string,
//  "value": string,
//  "isDelete": bool,
// }
bool verifyTransactionStructure(const json &transaction)
{
    if (transaction.contains("key") && transaction.contains("value") && transaction.contains("isDelete"))
    {
        if (transaction["key"].is_string() && transaction["value"].is_string() && transaction["isDelete"].is_boolean())
        {
            return true;
        }
    }
    return false;
}

void handle_request(const crow::request &req, crow::response &res, MutexQueue &queue)
{
    json data = {
        {"message", "Hello, World!"}};

    // Read data from request
    std::string requestData = req.body;
    std::cout << "Request data: " << requestData << std::endl;
    // Modify the data or perform any other operations
    // ...

    res.set_header("Content-Type", "application/json");

    json transactions;
    try{
        transactions=json::parse(requestData);
    } catch (const json::parse_error& e) {
        std::cerr << "Parsing error: " << e.what() << std::endl;
        res.code=500;
        res.end();
        return;
    }
    queue.push(transactions);
    res.end();
}

void sendPackage(MutexQueue &queue, std::mutex &mtx)
{
    std::thread threads[NUM_SERVERS]; // Static array of threads
    std::this_thread::sleep_for(std::chrono::milliseconds(DISPATCH_PERIOD));
    while (true)
    {
        // Read data from the source
        json package;
        queue.createPackage(package);
        std::lock_guard<std::mutex> lock(mtx);

        std::cout << package.dump() << std::endl;

        std::mutex writeMtx;
        for (int i = 0; i < NUM_SERVERS; i++)
        {
            threads[i] = std::thread([](json package, int i, std::mutex &writeMtx)
                                     {
                // Send the data to the server
                // ...
                std::this_thread::sleep_for(std::chrono::milliseconds(DISPATCH_PERIOD)); 
                std::lock_guard<std::mutex> lock(writeMtx);
                                std::cout<<"package sent to server "<<i<<std::endl; }, package, i, std::ref(writeMtx));
        }

        for (int i = 0; i < NUM_SERVERS; i++)
        {
            threads[i].join();
        }

        // Verify the data structure
        // ...
        // Add the data to the queue
        // ...
        std::this_thread::sleep_for(std::chrono::milliseconds(DISPATCH_PERIOD));
    }
}

/**
 * The main function is the entry point of the program.
 * This takes in the following mandatory arguments:
 * 1. BrokerID: The ID of the broker
 * 2.
 */
int main(int argc, char *argv[])
{
    crow::SimpleApp ingest;

    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " <BrokerID> missing" << std::endl;
        return 1;
    }
    int brokerID = std::stoi(argv[1]);
    std::mutex mtx;
    MutexQueue queue(brokerID, mtx);
    std::cout << "Starting broker with ID: " << brokerID << std::endl;
    CROW_ROUTE(ingest, "/")
        .name("hello")
        .methods("POST"_method)([&](const crow::request &req, crow::response &res)
                                { handle_request(req, res, queue); });
    std::thread packageThread(sendPackage, std::ref(queue), std::ref(mtx));
    packageThread.detach(); // This will terminate when the main thread exits, so no need to join.
    ingest.port(8080).multithreaded().run();
    return 0;
}